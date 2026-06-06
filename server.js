/**
 * YT7 Multi-Uploader v12.4 — Bugfixed
 * Fixes: job error status, scheduler cache, status batch Redis, IG polling
 * v12.4 additional fixes: channelJobs race, mget batching, TG markdown escape, TikTok error handling
 */
const express   = require('express');
const cors      = require('cors');
const fs        = require('fs');
const path      = require('path');
const { Redis }    = require('@upstash/redis');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');

const app  = express();
const PORT = process.env.PORT || 3000;
const TEMP_DIR = process.env.TEMP_DIR || '/tmp/yt7';
if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });

const redis = new Redis({
  url:   process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.set('Pragma', 'no-cache'); res.set('Expires', '0'); next();
});

const PLATFORMS = ['youtube', 'tiktok', 'instagram', 'facebook'];

// =============== CONFIG ===============
function buildDefaultConfig() {
  return {
    shared: {
      drive_client_id: process.env.DRIVE_CLIENT_ID || '',
      drive_client_secret: process.env.DRIVE_CLIENT_SECRET || '',
      drive_refresh_token: process.env.DRIVE_REFRESH_TOKEN || '',
      drive_api_key: process.env.DRIVE_API_KEY || '',
      yt_client_id: process.env.YT_CLIENT_ID || '',
      yt_client_secret: process.env.YT_CLIENT_SECRET || '',
      tv_client_id: process.env.TV_CLIENT_ID || '',
      tv_client_secret: process.env.TV_CLIENT_SECRET || '',
      yt_privacy: 'public',
      tiktok_client_key: process.env.TIKTOK_CLIENT_KEY || '',
      tiktok_client_secret: process.env.TIKTOK_CLIENT_SECRET || '',
      tiktok_privacy: 'SELF_ONLY',
      fb_app_id: process.env.FB_APP_ID || '',
      fb_app_secret: process.env.FB_APP_SECRET || '',
      base_url: process.env.BASE_URL || '',
      tg_bot_token: process.env.TG_BOT_TOKEN || '',
      tg_chat_id: process.env.TG_CHAT_ID || '',
    },
    channels: { youtube: [], tiktok: [], instagram: [], facebook: [] }
  };
}
function emptyChannel(platform, id) {
  const base = { id, platform, name: `${platform} #${id}`, drive_folder_id: '', enabled: false, schedule: [], titles: [], description: '', tags: '' };
  if (platform === 'youtube')   return { ...base, yt_refresh_token: '' };
  if (platform === 'tiktok')    return { ...base, tk_access_token:'', tk_refresh_token:'', tk_open_id:'', tk_token_expires_at:0, tk_privacy:'', tk_use_pull_from_url:true, tk_disable_duet:false, tk_disable_stitch:false, tk_disable_comment:false };
  if (platform === 'instagram') return { ...base, ig_user_id:'', ig_access_token:'', ig_share_to_feed:true };
  if (platform === 'facebook')  return { ...base, fb_page_id:'', fb_page_access_token:'' };
  return base;
}
function normalizeChannel(platform, raw = {}, fallbackId = 1) {
  const def = emptyChannel(platform, Number(raw.id) || fallbackId);
  const out = { ...def, ...raw };
  out.id = Number(raw.id) || fallbackId;
  out.platform = platform;
  out.name = String(raw.name || def.name);
  out.drive_folder_id = String(raw.drive_folder_id || '').trim();
  out.enabled = !!raw.enabled;
  out.schedule = normalizeScheduleSlots(raw.schedule);
  out.titles = normalizeTitlesList(raw.titles);
  out.description = String(raw.description || '');
  out.tags = Array.isArray(raw.tags) ? raw.tags : String(raw.tags || '');
  return out;
}
function normalizeConfig(raw = {}) {
  const def = buildDefaultConfig();
  const out = { shared: { ...def.shared, ...(raw.shared || {}) }, channels: { youtube:[], tiktok:[], instagram:[], facebook:[] } };
  for (const p of PLATFORMS) {
    const list = Array.isArray(raw.channels?.[p]) ? raw.channels[p] : [];
    out.channels[p] = list.map((ch, idx) => normalizeChannel(p, ch, idx + 1));
  }
  return out;
}
function normalizeTitlesList(input) {
  if (Array.isArray(input)) return input.map(t => String(t || '').trim()).filter(Boolean);
  if (typeof input === 'string') return input.split(/\r?\n/).map(t => t.trim()).filter(Boolean);
  return [];
}
function normalizeScheduleSlots(schedule) {
  if (!Array.isArray(schedule)) return [];
  const seen = new Set();
  return schedule.map(s => String(s || '').trim()).filter(s => /^\d{2}:\d{2}$/.test(s)).sort().filter(s => { if (seen.has(s)) return false; seen.add(s); return true; });
}

// =============== REDIS ===============
async function loadConfig() {
  let raw = await redis.get('config.json');
  if (raw && typeof raw === 'object' && raw.value !== undefined) raw = typeof raw.value === 'string' ? JSON.parse(raw.value) : raw.value;
  return normalizeConfig(raw || {});
}
async function saveConfig(cfg) {
  const normalized = normalizeConfig(cfg);
  await redis.set('config.json', normalized);
  // scheduler cache ও update করো — পরের tick এ stale data না যাক
  schedulerConfigCache = normalized;
  schedulerConfigCacheAt = Date.now();
}
async function loadChannelState(p, c) { return (await redis.get(`state:${p}:${c}`)) || {}; }
async function saveChannelState(p, c, s) { await redis.set(`state:${p}:${c}`, s); }
async function addLog(entry) {
  const newEntry = { ...entry, ts: new Date().toISOString() };
  await redis.lpush('upload_log_v2', JSON.stringify(newEntry));
  await redis.ltrim('upload_log_v2', 0, 299);
}
async function loadFired() { return (await redis.get('fired_slots')) || {}; }
async function saveFired(d) {
  const valid = new Set([bdDateOffset(-1), bdDateOffset(0), bdDateOffset(1)]);
  const clean = {};
  Object.entries(d).forEach(([k, v]) => { const date = k.split('_').pop(); if (valid.has(date)) clean[k] = v; });
  await redis.set('fired_slots', clean);
}
function findChannel(cfg, p, c) { return (cfg.channels[p] || []).find(x => Number(x.id) === Number(c)); }
function nextChannelId(cfg, p) { return (cfg.channels[p] || []).reduce((m, c) => Math.max(m, Number(c.id) || 0), 0) + 1; }

function bdTimeNow() {
  const bd = new Date(Date.now() + 6 * 60 * 60 * 1000);
  const hh = String(bd.getUTCHours()).padStart(2, '0');
  const mm = String(bd.getUTCMinutes()).padStart(2, '0');
  return { hh, mm, date: bd.toISOString().slice(0,10), current: `${hh}:${mm}`, totalMins: parseInt(hh)*60 + parseInt(mm) };
}
function bdDateOffset(days = 0) {
  const bd = new Date(Date.now() + days*86400000 + 6*60*60*1000);
  return bd.toISOString().slice(0, 10);
}

function normalizeChannelRotationState(raw = {}, allFileIds = []) {
  let rotation = Array.isArray(raw.rotation) ? raw.rotation : [];
  const ex = new Set(allFileIds); const seen = new Set();
  rotation = rotation.filter(id => ex.has(id) && !seen.has(id) && seen.add(id));
  for (const id of allFileIds) if (!seen.has(id)) { rotation.push(id); seen.add(id); }
  return { ...raw, rotation, title_index: Number.isInteger(raw.title_index) ? raw.title_index : 0, last_file_id: raw.last_file_id || null, last_used_at: raw.last_used_at || null, usage_count: Number.isInteger(raw.usage_count) ? raw.usage_count : 0 };
}
async function getNextFile(platform, chId, allFileIds) {
  const ordered = [...allFileIds].sort();
  const raw = await loadChannelState(platform, chId);
  const ch = normalizeChannelRotationState(raw, ordered);
  if (ch.rotation.length === 0) return { nextId: null, chState: ch };
  const nextId = ch.rotation.shift();
  ch.rotation.push(nextId);
  ch.last_file_id = nextId; ch.last_used_at = new Date().toISOString(); ch.usage_count += 1;
  return { nextId, chState: ch };
}

// =============== DRIVE ===============
async function refreshOAuthToken(clientId, clientSecret, refreshToken) {
  const r = await fetch('https://oauth2.googleapis.com/token', { method:'POST', body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type:'refresh_token' }) });
  const d = await r.json();
  if (!d.access_token) throw new Error('Token refresh failed: ' + JSON.stringify(d));
  return d;
}
async function getDriveToken(s) { return (await refreshOAuthToken(s.drive_client_id, s.drive_client_secret, s.drive_refresh_token)).access_token; }

async function listDriveFolder(folderId, driveToken, apiKey) {
  const q = encodeURIComponent(`'${folderId}' in parents and trashed=false`);
  let url, headers = {};
  if (apiKey) url = `https://www.googleapis.com/drive/v3/files?q=${q}&fields=files(id,name,size)&pageSize=200&key=${apiKey}`;
  else if (driveToken) { url = `https://www.googleapis.com/drive/v3/files?q=${q}&fields=files(id,name,size)&pageSize=200`; headers = { Authorization: `Bearer ${driveToken}` }; }
  else throw new Error('Drive API Key বা OAuth token কোনোটাই নেই');
  const r = await fetch(url, { headers }); const d = await r.json();
  if (!r.ok) throw new Error('Drive list failed: ' + JSON.stringify(d));
  const exts = /\.(mp4|mov|avi|mkv|webm)$/i;
  return (d.files || []).filter(f => exts.test(f.name)).sort((a, b) => String(a.name).localeCompare(String(b.name), undefined, { numeric: true, sensitivity: 'base' }));
}
async function downloadDriveFile(fileId, destPath, driveToken, apiKey) {
  let url, headers = {};
  if (apiKey) url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${apiKey}`;
  else if (driveToken) { url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`; headers = { Authorization: `Bearer ${driveToken}` }; }
  else throw new Error('Drive API Key বা OAuth token কোনোটাই নেই');
  let r = await fetch(url, { headers });
  if (!r.ok) { r = await fetch(`https://drive.google.com/uc?export=download&id=${fileId}`, { redirect: 'follow' }); if (!r.ok) throw new Error('Drive download failed: ' + fileId); }
  await pipeline(Readable.fromWeb(r.body), fs.createWriteStream(destPath));
  return destPath;
}
function drivePublicUrl(fileId, apiKey) {
  if (apiKey) return `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${apiKey}`;
  return `https://drive.google.com/uc?export=download&id=${fileId}`;
}

// =============== UPLOADERS ===============
async function getYTToken(ch, s) { return (await refreshOAuthToken(s.yt_client_id, s.yt_client_secret, ch.yt_refresh_token)).access_token; }

async function uploadToYouTube(videoPath, title, description, tags, ytToken, privacy = 'public') {
  const fileSize = fs.statSync(videoPath).size;
  const meta = { snippet: { title: String(title).substring(0,100), description: description || '', tags: Array.isArray(tags)?tags.slice(0,500):[], categoryId: '22' }, status: { privacyStatus: privacy, selfDeclaredMadeForKids: false } };
  const initR = await fetch('https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status', { method:'POST', headers: { Authorization: `Bearer ${ytToken}`, 'Content-Type':'application/json', 'X-Upload-Content-Type':'video/mp4', 'X-Upload-Content-Length': String(fileSize) }, body: JSON.stringify(meta) });
  if (!initR.ok) throw new Error('YT init failed: ' + await initR.text());
  const uploadUrl = initR.headers.get('location');
  const web = Readable.toWeb(fs.createReadStream(videoPath));
  const upR = await fetch(uploadUrl, { method:'PUT', headers:{'Content-Type':'video/mp4','Content-Length':String(fileSize)}, body: web, duplex:'half' });
  if (!upR.ok) throw new Error('YT upload failed: ' + await upR.text());
  return (await upR.json()).id;
}

async function refreshTikTokToken(s, ch) {
  const r = await fetch('https://open.tiktokapis.com/v2/oauth/token/', { method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body: new URLSearchParams({ client_key: s.tiktok_client_key, client_secret: s.tiktok_client_secret, grant_type:'refresh_token', refresh_token: ch.tk_refresh_token }) });
  const d = await r.json();
  if (!d.access_token) throw new Error('TikTok refresh failed: ' + JSON.stringify(d));
  return d;
}
async function ensureTikTokToken(cfg, ch) {
  const now = Math.floor(Date.now()/1000);
  if (ch.tk_access_token && ch.tk_token_expires_at > now + 60) return ch.tk_access_token;
  if (!ch.tk_refresh_token) throw new Error(`TikTok CH${ch.id}: refresh_token নেই — OAuth connect করো`);
  const tok = await refreshTikTokToken(cfg.shared, ch);
  ch.tk_access_token = tok.access_token;
  ch.tk_refresh_token = tok.refresh_token || ch.tk_refresh_token;
  ch.tk_token_expires_at = Math.floor(Date.now()/1000) + (tok.expires_in || 86400);
  const freshCfg = await loadConfig();
  const freshCh = findChannel(freshCfg, 'tiktok', ch.id);
  if (freshCh) {
    freshCh.tk_access_token = ch.tk_access_token;
    freshCh.tk_refresh_token = ch.tk_refresh_token;
    freshCh.tk_token_expires_at = ch.tk_token_expires_at;
    await saveConfig(freshCfg);
  }
  return ch.tk_access_token;
}
async function queryTikTokCreatorInfo(token) {
  const r = await fetch('https://open.tiktokapis.com/v2/post/publish/creator_info/query/', { method:'POST', headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json; charset=UTF-8' } });
  const d = await r.json();
  if (!r.ok) throw new Error('TikTok creator_info HTTP ' + r.status + ': ' + JSON.stringify(d));
  // FIX BUG4: (!d.error.code || ...) ছিল — code missing হলেও আগে throw হতো
  // এখন: error object আছে AND code 'ok' না হলেই throw
  if (d.error && d.error.code !== 'ok') throw new Error('TikTok creator_info: ' + (d.error.message || d.error.code || JSON.stringify(d.error)));
  return d.data || {};
}
async function uploadToTikTok(cfg, ch, fileInfo, title, token) {
  const info = await queryTikTokCreatorInfo(token);
  let privacy = ch.tk_privacy || cfg.shared.tiktok_privacy || 'SELF_ONLY';
  if (!(info.privacy_level_options || []).includes(privacy)) privacy = (info.privacy_level_options || ['SELF_ONLY'])[0];
  const postInfo = { title: String(title).slice(0,2200), privacy_level: privacy, disable_duet:!!ch.tk_disable_duet||!!info.duet_disabled, disable_stitch:!!ch.tk_disable_stitch||!!info.stitch_disabled, disable_comment:!!ch.tk_disable_comment||!!info.comment_disabled, brand_content_toggle:false, brand_organic_toggle:false };
  if (ch.tk_use_pull_from_url !== false && fileInfo.publicUrl) {
    const r = await fetch('https://open.tiktokapis.com/v2/post/publish/video/init/', { method:'POST', headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' }, body: JSON.stringify({ post_info: postInfo, source_info: { source:'PULL_FROM_URL', video_url: fileInfo.publicUrl } }) });
    const d = await r.json();
    // FIX BUG4: consistent error check
    if (d.error && d.error.code !== 'ok') throw new Error('TikTok init (PULL): ' + JSON.stringify(d.error));
    return d.data?.publish_id;
  }
  const stat = fs.statSync(fileInfo.localPath);
  const initR = await fetch('https://open.tiktokapis.com/v2/post/publish/video/init/', { method:'POST', headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' }, body: JSON.stringify({ post_info: postInfo, source_info: { source:'FILE_UPLOAD', video_size: stat.size, chunk_size: stat.size, total_chunk_count: 1 } }) });
  const initD = await initR.json();
  // FIX BUG4: consistent error check
  if (initD.error && initD.error.code !== 'ok') throw new Error('TikTok init (FILE): ' + JSON.stringify(initD.error));
  const uploadUrl = initD.data?.upload_url;
  if (!uploadUrl) throw new Error('TikTok init: no upload_url');
  const web = Readable.toWeb(fs.createReadStream(fileInfo.localPath));
  const putR = await fetch(uploadUrl, { method:'PUT', headers:{ 'Content-Type':'video/mp4', 'Content-Range':`bytes 0-${stat.size-1}/${stat.size}` }, body: web, duplex:'half' });
  if (!putR.ok) throw new Error('TikTok PUT failed: ' + await putR.text());
  return initD.data?.publish_id;
}

async function uploadToInstagram(ch, fileInfo, caption) {
  if (!ch.ig_user_id) throw new Error(`IG CH${ch.id}: ig_user_id নেই`);
  if (!ch.ig_access_token) throw new Error(`IG CH${ch.id}: access_token নেই`);
  if (!fileInfo.publicUrl) throw new Error(`IG CH${ch.id}: Drive public URL দরকার`);
  const params = new URLSearchParams({ media_type:'REELS', video_url: fileInfo.publicUrl, caption: String(caption||'').slice(0,2200), share_to_feed: ch.ig_share_to_feed === false ? 'false':'true', access_token: ch.ig_access_token });
  const cR = await fetch(`https://graph.facebook.com/v22.0/${ch.ig_user_id}/media`, { method:'POST', body: params });
  const cD = await cR.json();
  if (!cD.id) throw new Error('IG create container failed: ' + JSON.stringify(cD));
  const containerId = cD.id;
  const MAX_TRIES = 120;
  for (let tries = 0; tries < MAX_TRIES; tries++) {
    await new Promise(r => setTimeout(r, 5000));
    const sR = await fetch(`https://graph.facebook.com/v22.0/${containerId}?fields=status_code,status&access_token=${encodeURIComponent(ch.ig_access_token)}`);
    const sD = await sR.json();
    if (sD.status_code === 'FINISHED') break;
    if (sD.status_code === 'ERROR' || sD.status_code === 'EXPIRED') throw new Error(`IG container ${sD.status_code}: ${JSON.stringify(sD)}`);
    if (tries === MAX_TRIES - 1) throw new Error('IG container processing timeout (10min)');
  }
  const pR = await fetch(`https://graph.facebook.com/v22.0/${ch.ig_user_id}/media_publish`, { method:'POST', body: new URLSearchParams({ creation_id: containerId, access_token: ch.ig_access_token }) });
  const pD = await pR.json();
  if (!pD.id) throw new Error('IG publish failed: ' + JSON.stringify(pD));
  return pD.id;
}

async function uploadToFacebook(ch, fileInfo, title, description) {
  if (!ch.fb_page_id) throw new Error(`FB CH${ch.id}: page_id নেই`);
  if (!ch.fb_page_access_token) throw new Error(`FB CH${ch.id}: page access_token নেই`);
  if (!fileInfo.publicUrl) throw new Error(`FB CH${ch.id}: Drive public URL দরকার`);
  const r = await fetch(`https://graph.facebook.com/v22.0/${ch.fb_page_id}/videos`, { method:'POST', body: new URLSearchParams({ file_url: fileInfo.publicUrl, title: String(title||'').slice(0,255), description: String(description||'').slice(0,5000), access_token: ch.fb_page_access_token }) });
  const d = await r.json();
  if (!d.id) throw new Error('FB upload failed: ' + JSON.stringify(d));
  return d.id;
}

// FIX BUG3: Telegram Markdown special characters escape করো
function escapeMarkdown(text) {
  // Telegram MarkdownV1 এ এই characters problematic: _ * ` [
  return String(text || '').replace(/([_*`\[])/g, '\\$1');
}

async function tg(msg, isError = false) {
  const cfg = await loadConfig();
  const tok = cfg.shared.tg_bot_token; const chat = cfg.shared.tg_chat_id;
  if (!tok || !chat) return;
  const text = (isError ? '🚨 *ERROR*\n' : '✅ *Upload সফল!*\n') + msg;
  try { await fetch(`https://api.telegram.org/bot${tok}/sendMessage`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ chat_id: chat, text, parse_mode:'Markdown' }) }); } catch {}
}

// =============== CORE ===============
async function processChannel(platform, ch) {
  const cfg = await loadConfig();
  console.log(`\n[${platform.toUpperCase()}] ${ch.name} (CH${ch.id})`);
  if (!ch.drive_folder_id) throw new Error(`${platform} CH${ch.id}: Drive folder ID নেই`);
  const apiKey = cfg.shared.drive_api_key || '';
  let driveToken = null;
  if (!apiKey) {
    if (!cfg.shared.drive_refresh_token) throw new Error('Drive API Key বা OAuth token কোনোটাই নেই');
    driveToken = await getDriveToken(cfg.shared);
  }
  const files = await listDriveFolder(ch.drive_folder_id, driveToken, apiKey);
  if (!files.length) throw new Error('Drive folder-এ কোনো ভিডিও নেই');
  const allIds = files.map(f => f.id);
  const { nextId, chState } = await getNextFile(platform, ch.id, allIds);
  if (!nextId) throw new Error('Queue empty');
  const file = files.find(f => f.id === nextId);
  const tempPath = path.join(TEMP_DIR, `${platform}_ch${ch.id}_${Date.now()}.mp4`);
  const titles = (ch.titles || []).filter(t => t && t.trim());
  let title;
  if (titles.length === 0) title = file.name.replace(/\.[^.]+$/, '').replace(/[_-]/g, ' ').trim() || ch.name;
  else if (titles.length === 1) title = titles[0];
  else { const ti = (chState.title_index || 0) % titles.length; title = titles[ti]; chState.title_index = ti + 1; }
  const description = ch.description?.trim() || '';
  let tags = [];
  if (ch.tags) { try { const parsed = typeof ch.tags === 'string' ? JSON.parse(ch.tags) : ch.tags; tags = Array.isArray(parsed) ? parsed : []; } catch { tags = String(ch.tags).split(',').map(t => t.trim()).filter(Boolean); } }
  const needsLocal = (platform === 'youtube') || (platform === 'tiktok' && ch.tk_use_pull_from_url === false);
  const fileInfo = { id: nextId, name: file.name, publicUrl: drivePublicUrl(nextId, apiKey), localPath: null };
  if (needsLocal) { console.log(`  ⬇ Downloading: ${file.name}`); await downloadDriveFile(nextId, tempPath, driveToken, apiKey); fileInfo.localPath = tempPath; }
  else console.log(`  🔗 Using Drive public URL`);

  let resultId, resultUrl;
  try {
    if (platform === 'youtube') {
      const ytToken = await getYTToken(ch, cfg.shared);
      resultId = await uploadToYouTube(tempPath, title.substring(0,100), description, tags, ytToken, cfg.shared.yt_privacy || 'public');
      resultUrl = `https://youtu.be/${resultId}`;
    } else if (platform === 'tiktok') {
      const tkToken = await ensureTikTokToken(cfg, ch);
      resultId = await uploadToTikTok(cfg, ch, fileInfo, title, tkToken);
      resultUrl = `tiktok:publish_id=${resultId}`;
    } else if (platform === 'instagram') {
      const tagSuffix = tags.length ? '\n\n' + tags.map(t => '#' + String(t).replace(/[^a-zA-Z0-9_]/g, '')).join(' ') : '';
      const caption = title + (description ? '\n\n' + description : '') + tagSuffix;
      resultId = await uploadToInstagram(ch, fileInfo, caption);
      resultUrl = `https://www.instagram.com/reel/${resultId}/`;
    } else if (platform === 'facebook') {
      resultId = await uploadToFacebook(ch, fileInfo, title, description);
      resultUrl = `https://www.facebook.com/${ch.fb_page_id}/videos/${resultId}`;
    } else throw new Error('Unknown platform: ' + platform);
  } finally { if (fileInfo.localPath) { try { fs.unlinkSync(fileInfo.localPath); } catch {} } }
  await saveChannelState(platform, ch.id, chState);
  await addLog({ platform, chId: ch.id, channel: ch.name, title, videoId: resultId, url: resultUrl, file: file.name, status: 'ok' });
  // FIX BUG3: title ও channel name escape করো Telegram markdown injection রোধে
  await tg(`Platform: *${platform}*\nChannel: *${escapeMarkdown(ch.name)}*\nTitle: \`${escapeMarkdown(title)}\`\nLink: ${resultUrl}\nFile: \`${escapeMarkdown(file.name)}\``);
  console.log(`  🎉 Done! ${resultUrl}`);
  return { resultId, resultUrl, title, file: file.name };
}

// =============== JOBS ===============
const jobs = {};
const jobTimers = {};
function newJob() {
  const id = `job_${Date.now()}_${Math.random().toString(36).slice(2,7)}`;
  jobs[id] = { status:'running', log:[], started: new Date().toISOString(), total:0, ok:0, failed:0 };
  jobTimers[id] = setTimeout(() => { delete jobs[id]; delete jobTimers[id]; }, 6*60*60*1000);
  return id;
}
function finishJob(id) {
  if (jobTimers[id]) { clearTimeout(jobTimers[id]); delete jobTimers[id]; }
  setTimeout(() => delete jobs[id], 10*60*1000);
}
function jobLog(id, msg) {
  console.log(msg);
  if (jobs[id]) jobs[id].log.push(`[${new Date().toLocaleTimeString()}] ${msg}`);
}

// FIX BUG1: channelJobs race condition — atomic mutex pattern
// আগে: check করে তারপর set করতো — দুটো concurrent call একই সাথে check পার হতে পারত
// এখন: একটা in-memory Map-কে mutex হিসেবে ব্যবহার করা হচ্ছে।
// set করার আগেই check, set, এবং তারপর বাকি কাজ — সব synchronous, কোনো await নেই মাঝে।
const channelJobs = {};

async function runUploadJob(targets) {
  const jobId = newJob();
  const cfg = await loadConfig();
  const promises = targets.map(async ({ platform, chId }) => {
    const ch = findChannel(cfg, platform, chId);
    const key = `${platform}:${chId}`;
    if (!ch || !ch.enabled) { jobLog(jobId, `⏭ ${key} disabled/notfound`); return { platform, chId, status:'skip' }; }

    // FIX BUG1: check + set atomically (no await between them)
    // Node.js event loop-এ এই দুই line এর মাঝে অন্য কোনো microtask চলতে পারবে না
    if (channelJobs[key]) { jobLog(jobId, `⏭ ${key} already running`); return { platform, chId, status:'already_running' }; }
    channelJobs[key] = jobId; // ← এখনই lock করো, কোনো await ছাড়া

    if (jobs[jobId]) jobs[jobId].total++;
    try {
      const MAX_RETRY = 3;
      const RETRY_DELAY = 30 * 1000;
      let lastError = null;
      for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
        try {
          if (attempt > 1) {
            jobLog(jobId, `🔄 ${key} retry ${attempt}/${MAX_RETRY} (30s পরে)...`);
            await new Promise(r => setTimeout(r, RETRY_DELAY));
          } else {
            jobLog(jobId, `▶ ${key} (${ch.name}) শুরু...`);
          }
          const r = await processChannel(platform, ch);
          jobLog(jobId, `✅ ${key} সফল: ${r.resultUrl}`);
          if (jobs[jobId]) jobs[jobId].ok++;
          return { platform, chId, status:'ok', ...r };
        } catch (e) {
          lastError = e;
          const noRetry = e.message.includes('401') || e.message.includes('invalid_token') ||
            e.message.includes('token') && e.message.includes('নেই') ||
            e.message.includes('folder ID নেই') || e.message.includes('Queue empty') ||
            e.message.includes('Drive folder-এ কোনো ভিডিও নেই');
          if (noRetry || attempt === MAX_RETRY) break;
          jobLog(jobId, `⚠️ ${key} attempt ${attempt} failed: ${e.message.slice(0,100)}`);
        }
      }
      jobLog(jobId, `❌ ${key} failed (${MAX_RETRY} tries): ${lastError.message}`);
      if (jobs[jobId]) jobs[jobId].failed++;
      await addLog({ platform, chId, channel: ch?.name || key, status:'error', error: lastError.message });
      await tg(`Platform: *${platform}*\nChannel: *${escapeMarkdown(ch?.name || key)}*\nError: \`${escapeMarkdown(lastError.message.slice(0,200))}\``, true);
      return { platform, chId, status:'error', error: lastError.message };
    } finally { delete channelJobs[key]; }
  });
  Promise.all(promises).then(results => {
    if (!jobs[jobId]) return;
    jobs[jobId].results = results;
    const failed = results.filter(r => r.status === 'error').length;
    const ok     = results.filter(r => r.status === 'ok').length;
    if (failed === 0)          jobs[jobId].status = 'done';
    else if (ok === 0)         jobs[jobId].status = 'error';
    else                       jobs[jobId].status = 'partial';
    finishJob(jobId);
  }).catch(err => {
    if (jobs[jobId]) { jobs[jobId].status = 'error'; jobLog(jobId, '❌ Fatal: ' + err.message); finishJob(jobId); }
  });
  return jobId;
}

// =============== SCHEDULER — with config cache ===============
let schedulerConfigCache = null;
let schedulerConfigCacheAt = 0;
const SCHEDULER_CACHE_TTL = 2 * 60 * 1000;

async function loadConfigCached() {
  if (schedulerConfigCache && (Date.now() - schedulerConfigCacheAt) < SCHEDULER_CACHE_TTL) {
    return schedulerConfigCache;
  }
  schedulerConfigCache = await loadConfig();
  schedulerConfigCacheAt = Date.now();
  return schedulerConfigCache;
}

let schedulerTimer = null, schedulerTickRunning = false;
function collectDueScheduleBatches(cfg, fired, nowInfo = bdTimeNow()) {
  const { totalMins, date } = nowInfo;
  const grouped = new Map(); let changed = false;
  for (const platform of PLATFORMS) for (const ch of (cfg.channels[platform] || [])) {
    if (!ch.enabled || !ch.schedule?.length) continue;
    for (const slot of normalizeScheduleSlots(ch.schedule)) {
      const [sHH, sMM] = slot.split(':').map(Number);
      const slotMins = sHH * 60 + sMM;
      const diff = totalMins - slotMins;
      if (diff < 0 || diff > 2) continue;
      const fireKey = `${platform}_${ch.id}_${slot}_${date}`;
      if (fired[fireKey]) continue;
      fired[fireKey] = new Date().toISOString(); changed = true;
      const slotKey = `${slot}_${date}`;
      if (!grouped.has(slotKey)) grouped.set(slotKey, { slot, date, targets: [], names: [] });
      grouped.get(slotKey).targets.push({ platform, chId: ch.id });
      grouped.get(slotKey).names.push(`${platform}:${ch.name}`);
    }
  }
  return { batches: [...grouped.values()], changed, fired };
}
async function schedulerTick() {
  if (schedulerTickRunning) return;
  schedulerTickRunning = true;
  try {
    const cfg = await loadConfigCached();
    const fired = await loadFired();
    const { batches, changed } = collectDueScheduleBatches(cfg, fired);
    if (changed) await saveFired(fired);
    for (const batch of batches) {
      console.log(`[SCHED] ⏰ ${batch.slot} → ${batch.names.join(', ')}`);
      runUploadJob(batch.targets);
    }
  } finally { schedulerTickRunning = false; }
}
function startScheduler() {
  if (schedulerTimer) clearInterval(schedulerTimer);
  schedulerTick().catch(err => console.error('[SCHED] tick:', err.message));
  schedulerTimer = setInterval(() => { schedulerTick().catch(err => console.error('[SCHED] tick:', err.message)); }, 30*1000);
  console.log('📅 Scheduler started (30s tick, 2min config cache, ±2min window)');
}

// =============== OAUTH ===============
function connectedHTML(platform, chId, name, extra='') { return `<html><body style="background:#0f172a;color:#22c55e;font-family:sans-serif;text-align:center;padding:60px"><div style="font-size:56px">✅</div><h2>${platform} Channel ${chId} Connected!</h2><p style="color:#94a3b8">${name}</p>${extra?`<p style="color:#64748b;margin-top:12px">${extra}</p>`:''}<p style="color:#64748b;margin-top:16px">এই পেজ বন্ধ করো</p><script>setTimeout(()=>window.close(),3500)</script></body></html>`; }
function errorHTML(msg) { return `<html><body style="background:#0f172a;color:#ef4444;padding:40px;font-family:sans-serif"><h2>Error</h2><p style="color:#fca5a5">${String(msg).replace(/</g,'&lt;')}</p></body></html>`; }

app.get('/auth/youtube/:chId/start', async (req, res) => {
  const cfg = await loadConfig(); const chId = parseInt(req.params.chId);
  const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
  res.redirect(`https://accounts.google.com/o/oauth2/v2/auth?client_id=${cfg.shared.yt_client_id}&redirect_uri=${base}/auth/youtube/${chId}/callback&response_type=code&scope=${encodeURIComponent('https://www.googleapis.com/auth/youtube.upload')}&access_type=offline&prompt=consent&state=${chId}`);
});
app.get('/auth/youtube/:chId/callback', async (req, res) => {
  const { code, state } = req.query; const chId = parseInt(state || req.params.chId);
  try {
    const cfg = await loadConfig(); const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
    const r = await fetch('https://oauth2.googleapis.com/token', { method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body: new URLSearchParams({ code, client_id: cfg.shared.yt_client_id, client_secret: cfg.shared.yt_client_secret, redirect_uri:`${base}/auth/youtube/${chId}/callback`, grant_type:'authorization_code' }) });
    const tokens = await r.json();
    if (!tokens.refresh_token) throw new Error('No refresh_token: ' + JSON.stringify(tokens));
    const ch = findChannel(cfg, 'youtube', chId);
    if (!ch) throw new Error('Channel not found: ' + chId);
    ch.yt_refresh_token = tokens.refresh_token;
    try { const chR = await fetch('https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true', { headers:{ Authorization:`Bearer ${tokens.access_token}` } }); const chD = await chR.json(); if (chD.items?.[0]?.snippet?.title) ch.name = chD.items[0].snippet.title; } catch {}
    await saveConfig(cfg);
    res.send(connectedHTML('YouTube', chId, ch.name));
  } catch (e) { res.send(errorHTML(e.message)); }
});

app.post('/api/device/start/:chId', async (req, res) => {
  const cfg = await loadConfig();
  if (!cfg.shared.tv_client_id) return res.status(400).json({ error: 'TV Client ID দাও Settings-এ' });
  const r = await fetch('https://oauth2.googleapis.com/device/code', { method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body: new URLSearchParams({ client_id: cfg.shared.tv_client_id, scope:'https://www.googleapis.com/auth/youtube.upload' }) });
  const d = await r.json();
  if (!r.ok) return res.status(400).json({ error: d.error_description || JSON.stringify(d) });
  res.json({ device_code: d.device_code, user_code: d.user_code, verification_url: d.verification_url, interval: d.interval || 5, expires_in: d.expires_in });
});
app.post('/api/device/poll/:chId', async (req, res) => {
  const cfg = await loadConfig(); const chId = parseInt(req.params.chId);
  const { device_code } = req.body;
  if (!cfg.shared.tv_client_id || !cfg.shared.tv_client_secret) return res.status(400).json({ error:'TV Client ID/Secret দাও' });
  if (!device_code) return res.status(400).json({ error:'device_code দাও' });
  const r = await fetch('https://oauth2.googleapis.com/token', { method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body: new URLSearchParams({ client_id: cfg.shared.tv_client_id, client_secret: cfg.shared.tv_client_secret, device_code, grant_type:'urn:ietf:params:oauth:grant-type:device_code' }) });
  const d = await r.json();
  if (d.error === 'authorization_pending') return res.json({ status:'pending' });
  if (d.error === 'slow_down') return res.json({ status:'slow_down' });
  if (d.error) return res.status(400).json({ error: d.error_description || d.error });
  if (d.refresh_token) {
    const ch = findChannel(cfg, 'youtube', chId);
    if (!ch) return res.status(404).json({ error:'Channel not found' });
    ch.yt_refresh_token = d.refresh_token;
    try { const chR = await fetch('https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true', { headers:{ Authorization:`Bearer ${d.access_token}` } }); const chD = await chR.json(); if (chD.items?.[0]?.snippet?.title) ch.name = chD.items[0].snippet.title; } catch {}
    await saveConfig(cfg);
    return res.json({ status:'ok', channel_name: ch.name });
  }
  res.status(400).json({ error:'Token পাওয়া যায়নি' });
});

app.get('/auth/tiktok/:chId/start', async (req, res) => {
  const cfg = await loadConfig(); const chId = parseInt(req.params.chId);
  const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
  res.redirect(`https://www.tiktok.com/v2/auth/authorize/?client_key=${encodeURIComponent(cfg.shared.tiktok_client_key)}&scope=${encodeURIComponent('user.info.basic,video.publish,video.upload')}&response_type=code&redirect_uri=${encodeURIComponent(base + '/auth/tiktok/' + chId + '/callback')}&state=${chId}`);
});
app.get('/auth/tiktok/:chId/callback', async (req, res) => {
  const { code, state, error } = req.query;
  if (error) return res.send(errorHTML('TikTok: ' + error));
  const chId = parseInt(state || req.params.chId);
  try {
    const cfg = await loadConfig(); const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
    const r = await fetch('https://open.tiktokapis.com/v2/oauth/token/', { method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body: new URLSearchParams({ client_key: cfg.shared.tiktok_client_key, client_secret: cfg.shared.tiktok_client_secret, code, grant_type:'authorization_code', redirect_uri:`${base}/auth/tiktok/${chId}/callback` }) });
    const tokens = await r.json();
    if (!tokens.access_token) throw new Error('TikTok no access_token: ' + JSON.stringify(tokens));
    const ch = findChannel(cfg, 'tiktok', chId);
    if (!ch) throw new Error('TikTok channel not found: ' + chId);
    ch.tk_access_token = tokens.access_token; ch.tk_refresh_token = tokens.refresh_token; ch.tk_open_id = tokens.open_id;
    ch.tk_token_expires_at = Math.floor(Date.now()/1000) + (tokens.expires_in || 86400);
    try { const info = await queryTikTokCreatorInfo(tokens.access_token); if (info.creator_nickname) ch.name = info.creator_nickname; } catch {}
    await saveConfig(cfg);
    res.send(connectedHTML('TikTok', chId, ch.name));
  } catch (e) { res.send(errorHTML(e.message)); }
});

app.get('/auth/facebook/:chId/start', async (req, res) => {
  const cfg = await loadConfig(); const chId = parseInt(req.params.chId);
  const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
  res.redirect(`https://www.facebook.com/v22.0/dialog/oauth?client_id=${encodeURIComponent(cfg.shared.fb_app_id)}&redirect_uri=${encodeURIComponent(base + '/auth/facebook/' + chId + '/callback')}&scope=${encodeURIComponent('pages_show_list,pages_manage_posts,pages_read_engagement,publish_video,business_management')}&state=${chId}&response_type=code`);
});
app.get('/auth/facebook/:chId/callback', async (req, res) => {
  const { code, state, error } = req.query;
  if (error) return res.send(errorHTML('FB: ' + error));
  const chId = parseInt(state || req.params.chId);
  try {
    const cfg = await loadConfig(); const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
    const tR = await fetch(`https://graph.facebook.com/v22.0/oauth/access_token?` + new URLSearchParams({ client_id: cfg.shared.fb_app_id, client_secret: cfg.shared.fb_app_secret, redirect_uri:`${base}/auth/facebook/${chId}/callback`, code }));
    const tD = await tR.json();
    if (!tD.access_token) throw new Error('FB token: ' + JSON.stringify(tD));
    const llR = await fetch(`https://graph.facebook.com/v22.0/oauth/access_token?` + new URLSearchParams({ grant_type:'fb_exchange_token', client_id: cfg.shared.fb_app_id, client_secret: cfg.shared.fb_app_secret, fb_exchange_token: tD.access_token }));
    const llD = await llR.json();
    const userToken = llD.access_token || tD.access_token;
    const pR = await fetch(`https://graph.facebook.com/v22.0/me/accounts?access_token=${encodeURIComponent(userToken)}`);
    const pD = await pR.json();
    if (!pD.data?.length) throw new Error('No FB pages found.');
    const page = pD.data[0];
    const ch = findChannel(cfg, 'facebook', chId);
    if (!ch) throw new Error('FB channel not found: ' + chId);
    ch.fb_page_id = page.id; ch.fb_page_access_token = page.access_token; ch.name = page.name || ch.name;
    await saveConfig(cfg);
    res.send(connectedHTML('Facebook', chId, ch.name, `Page: <b>${page.name}</b><br>Page ID: <code>${page.id}</code>`));
  } catch (e) { res.send(errorHTML(e.message)); }
});

app.get('/auth/instagram/:chId/start', async (req, res) => {
  const cfg = await loadConfig(); const chId = parseInt(req.params.chId);
  const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
  res.redirect(`https://www.facebook.com/v22.0/dialog/oauth?client_id=${encodeURIComponent(cfg.shared.fb_app_id)}&redirect_uri=${encodeURIComponent(base + '/auth/instagram/' + chId + '/callback')}&scope=${encodeURIComponent('instagram_basic,instagram_content_publish,pages_show_list,pages_read_engagement,business_management')}&state=${chId}&response_type=code`);
});
app.get('/auth/instagram/:chId/callback', async (req, res) => {
  const { code, state, error } = req.query;
  if (error) return res.send(errorHTML('IG: ' + error));
  const chId = parseInt(state || req.params.chId);
  try {
    const cfg = await loadConfig(); const base = cfg.shared.base_url || `${req.protocol}://${req.get('host')}`;
    const tR = await fetch(`https://graph.facebook.com/v22.0/oauth/access_token?` + new URLSearchParams({ client_id: cfg.shared.fb_app_id, client_secret: cfg.shared.fb_app_secret, redirect_uri:`${base}/auth/instagram/${chId}/callback`, code }));
    const tD = await tR.json();
    if (!tD.access_token) throw new Error('FB token: ' + JSON.stringify(tD));
    const llR = await fetch(`https://graph.facebook.com/v22.0/oauth/access_token?` + new URLSearchParams({ grant_type:'fb_exchange_token', client_id: cfg.shared.fb_app_id, client_secret: cfg.shared.fb_app_secret, fb_exchange_token: tD.access_token }));
    const llD = await llR.json();
    const userToken = llD.access_token || tD.access_token;
    const pR = await fetch(`https://graph.facebook.com/v22.0/me/accounts?fields=id,name,access_token,instagram_business_account&access_token=${encodeURIComponent(userToken)}`);
    const pD = await pR.json();
    if (!pD.data?.length) throw new Error('No FB pages found.');
    const pageWithIG = pD.data.find(p => p.instagram_business_account);
    if (!pageWithIG) throw new Error('IG Business account connected page পাওয়া যায়নি। FB Page → Linked Accounts থেকে link কর।');
    const ch = findChannel(cfg, 'instagram', chId);
    if (!ch) throw new Error('IG channel not found: ' + chId);
    ch.ig_user_id = pageWithIG.instagram_business_account.id;
    ch.ig_access_token = pageWithIG.access_token;
    try { const ur = await fetch(`https://graph.facebook.com/v22.0/${ch.ig_user_id}?fields=username&access_token=${encodeURIComponent(ch.ig_access_token)}`); const ud = await ur.json(); if (ud.username) ch.name = '@' + ud.username; } catch {}
    await saveConfig(cfg);
    res.send(connectedHTML('Instagram', chId, ch.name, `IG User ID: <code>${ch.ig_user_id}</code>`));
  } catch (e) { res.send(errorHTML(e.message)); }
});

// =============== REST API ===============
function maskSensitive(safe) {
  const mask = v => v ? '••••' + String(v).slice(-4) : '';
  const s = safe.shared;
  ['yt_client_secret','drive_client_secret','drive_refresh_token','tg_bot_token','tv_client_secret','tiktok_client_secret','fb_app_secret','drive_api_key'].forEach(k => { if (s[k]) s[k] = mask(s[k]); });
  for (const p of PLATFORMS) (safe.channels[p] || []).forEach(ch => {
    if (ch.yt_refresh_token) ch.yt_refresh_token = '••••connected';
    if (ch.tk_refresh_token) ch.tk_refresh_token = '••••connected';
    if (ch.tk_access_token) ch.tk_access_token = '••••' + String(ch.tk_access_token).slice(-4);
    if (ch.ig_access_token) ch.ig_access_token = '••••connected';
    if (ch.fb_page_access_token) ch.fb_page_access_token = '••••connected';
  });
  return safe;
}

app.get('/api/config', async (req, res) => { res.json(maskSensitive(JSON.parse(JSON.stringify(await loadConfig())))); });
app.post('/api/config/shared', async (req, res) => {
  const cfg = await loadConfig();
  Object.entries(req.body).forEach(([k, v]) => { if (v !== undefined && v !== null && !String(v).includes('••••')) cfg.shared[k] = v; });
  await saveConfig(cfg); res.json({ ok: true });
});
app.post('/api/channel/:platform/add', async (req, res) => {
  const platform = req.params.platform;
  if (!PLATFORMS.includes(platform)) return res.status(400).json({ error:'Invalid platform' });
  const cfg = await loadConfig();
  const ch = emptyChannel(platform, nextChannelId(cfg, platform));
  cfg.channels[platform].push(ch);
  await saveConfig(cfg); res.json({ ok:true, channel: ch });
});
app.delete('/api/channel/:platform/:chId', async (req, res) => {
  const platform = req.params.platform; const chId = parseInt(req.params.chId);
  if (!PLATFORMS.includes(platform)) return res.status(400).json({ error:'Invalid platform' });
  const cfg = await loadConfig();
  cfg.channels[platform] = (cfg.channels[platform] || []).filter(c => c.id !== chId);
  await saveConfig(cfg); await redis.del(`state:${platform}:${chId}`);
  res.json({ ok:true });
});
app.post('/api/channel/:platform/:chId', async (req, res) => {
  const platform = req.params.platform; const chId = parseInt(req.params.chId);
  if (!PLATFORMS.includes(platform)) return res.status(400).json({ error:'Invalid platform' });
  const cfg = await loadConfig();
  const ch = findChannel(cfg, platform, chId);
  if (!ch) return res.status(404).json({ error:'Channel not found' });
  const body = req.body || {};
  const writable = ['name','drive_folder_id','enabled','schedule','titles','description','tags','yt_refresh_token','tk_refresh_token','tk_access_token','tk_open_id','tk_privacy','tk_use_pull_from_url','tk_disable_duet','tk_disable_stitch','tk_disable_comment','ig_user_id','ig_access_token','ig_share_to_feed','fb_page_id','fb_page_access_token'];
  for (const k of writable) {
    if (body[k] === undefined) continue;
    if (typeof body[k] === 'string' && body[k].includes('••••')) continue;
    if (k === 'schedule') { ch[k] = normalizeScheduleSlots(body[k]); continue; }
    if (k === 'titles') { ch[k] = normalizeTitlesList(body[k]); continue; }
    if (k === 'tags') { ch[k] = Array.isArray(body[k]) ? body[k] : String(body[k] || ''); continue; }
    ch[k] = body[k];
  }
  await saveConfig(cfg);
  res.json({ ok:true, channel: findChannel(await loadConfig(), platform, chId) });
});

app.post('/api/upload/:platform/:chId', async (req, res) => { res.json({ jobId: await runUploadJob([{ platform: req.params.platform, chId: parseInt(req.params.chId) }]) }); });
app.post('/api/upload/platform/:platform/all', async (req, res) => {
  const platform = req.params.platform; const cfg = await loadConfig();
  const targets = (cfg.channels[platform] || []).filter(c => c.enabled).map(c => ({ platform, chId: c.id }));
  if (!targets.length) return res.status(400).json({ error:'কোনো enabled channel নেই' });
  res.json({ jobId: await runUploadJob(targets) });
});
app.post('/api/upload/all', async (req, res) => {
  const cfg = await loadConfig(); const targets = [];
  for (const p of PLATFORMS) (cfg.channels[p] || []).filter(c => c.enabled).forEach(c => targets.push({ platform: p, chId: c.id }));
  if (!targets.length) return res.status(400).json({ error:'কোনো enabled channel নেই' });
  res.json({ jobId: await runUploadJob(targets) });
});

app.get('/api/job/:id', (req, res) => {
  const j = jobs[req.params.id];
  if (!j) return res.status(404).json({ error:'Job not found' });
  res.json(j);
});

app.get('/api/logs', async (req, res) => { const raw = await redis.lrange('upload_log_v2', 0, 299); res.json(raw.map(r => typeof r === 'string' ? JSON.parse(r) : r)); });
app.post('/api/state/reset/:platform/:chId', async (req, res) => { await redis.del(`state:${req.params.platform}:${req.params.chId}`); res.json({ ok:true }); });
app.post('/api/telegram/test', async (req, res) => { try { await tg('✅ Telegram connected! YT7 v12 চালু আছে 🎉'); res.json({ ok:true }); } catch (e) { res.status(500).json({ error: e.message }); } });

// FIX BUG2: redis.mget 100 key limit — চ্যানেল বেশি হলে crash হতো
// এখন 100 key-এর batch-এ ভাগ করে mget করা হয়
async function redisMgetBatched(keys, batchSize = 100) {
  if (keys.length === 0) return [];
  const results = [];
  for (let i = 0; i < keys.length; i += batchSize) {
    const batch = keys.slice(i, i + batchSize);
    const batchResults = await redis.mget(...batch);
    results.push(...batchResults);
  }
  return results;
}

app.get('/api/status', async (req, res) => {
  const cfg = await loadConfig();
  const rawLog = await redis.lrange('upload_log_v2', 0, 299); const log = rawLog.map(r => typeof r === 'string' ? JSON.parse(r) : r);
  const out = { platforms: {}, todayUploads: 0, totalUploads: 0 };

  const allKeys = [];
  for (const p of PLATFORMS) for (const ch of (cfg.channels[p] || [])) allKeys.push(`state:${p}:${ch.id}`);

  // FIX BUG2: batched mget — 100+ key হলে আগে crash হতো
  let allStates = [];
  if (allKeys.length > 0) {
    try { allStates = await redisMgetBatched(allKeys); } catch (e) {
      console.error('[STATUS] Redis mget failed:', e.message);
      allStates = new Array(allKeys.length).fill(null);
      out.redisError = 'Channel state লোড হয়নি — Redis connection সমস্যা';
    }
  }
  let keyIdx = 0;
  for (const p of PLATFORMS) {
    out.platforms[p] = (cfg.channels[p] || []).map(ch => {
      const st = allStates[keyIdx++] || {};
      const rotation = Array.isArray(st.rotation) ? st.rotation : [];
      const hasToken = p==='youtube'?!!ch.yt_refresh_token : p==='tiktok'?!!ch.tk_refresh_token : p==='instagram'?!!ch.ig_access_token : p==='facebook'?!!ch.fb_page_access_token : false;
      return { id: ch.id, name: ch.name, enabled: ch.enabled, hasToken, hasDrive: !!ch.drive_folder_id, schedule: ch.schedule, queueLeft: rotation.length, totalDone: Number.isInteger(st.usage_count) ? st.usage_count : 0 };
    });
  }
  const todayBD = bdDateOffset(0);
  out.todayUploads = log.filter(l => l.ts?.startsWith(todayBD) && l.status === 'ok').length;
  out.totalUploads = log.filter(l => l.status === 'ok').length;
  res.json(out);
});

app.get('/api/time', async (req, res) => {
  const { current, date, totalMins } = bdTimeNow();
  const cfg = await loadConfigCached();
  const fired = await loadFired();
  let nextSlots = [];
  for (const p of PLATFORMS) for (const ch of (cfg.channels[p] || [])) {
    if (!ch.enabled || !ch.schedule?.length) continue;
    for (const slot of ch.schedule) {
      const [sHH, sMM] = slot.split(':').map(Number);
      const diff = (sHH * 60 + sMM) - totalMins;
      nextSlots.push({ platform: p, ch: ch.name, slot, diff: diff < 0 ? diff + 1440 : diff, done: !!fired[`${p}_${ch.id}_${slot}_${date}`] });
    }
  }
  nextSlots.sort((a, b) => a.diff - b.diff);
  const next = nextSlots.find(s => !s.done);
  res.json({ bd: `${current} (${date})`, utc: new Date().toISOString(), next_upload: next ? `[${next.platform}] ${next.ch} @ ${next.slot} (${next.diff}m পরে)` : 'কোনো schedule নেই' });
});

app.get('/version', (req, res) => res.json({ version:'v12.4', build:'bugfixed-r2', platforms: PLATFORMS, fixes: ['job-error-status','scheduler-cache','status-mget','ig-timeout','tiktok-error-detect','tiktok-token-race','fb-api-v22','channeljobs-race','mget-batching','tg-markdown-escape','tiktok-error-consistency'] }));

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`🚀 YT7 Multi-Uploader v12.4 (bugfixed-r2) on port ${PORT}`);
    startScheduler();
  });
} else {
  module.exports = { buildDefaultConfig, normalizeConfig };
}