# 🎬 YT7 Multi-Uploader v12

**YouTube + TikTok + Instagram + Facebook** auto-uploader from Google Drive.

v11.2 (YouTube only) থেকে upgrade — এখন ৪টা platform-এ post করতে পারে, প্রতি platform-এ যত খুশি channel/account add করা যায়।

## ✅ ফিচার
- 🎯 **4 platforms:** YouTube, TikTok, Instagram (Reels), Facebook Page
- 🔧 **Dynamic channels:** প্রতি platform-এ ইচ্ছামত add/remove
- 🎚 **Per-channel toggle:** কোন কোন channel on থাকবে control করার switch
- 📁 **Single Drive source:** সব platform Google Drive folder থেকে video নেয়
- ⏰ **BD time scheduler:** প্রতি channel-এর আলাদা schedule
- 🔄 **Cycling rotation:** সব video শেষ হলে আবার প্রথম থেকে
- 🔐 **OAuth connect button** প্রতি platform-এর জন্য
- 📨 **Telegram notification**

## 🚀 Railway Deploy
1. GitHub-এ এই ফোল্ডার push করো
2. Railway → New Project → GitHub repo
3. Add Upstash Redis plugin (অথবা Upstash account থেকে credentials)
4. ENV Tab → Raw Editor → `.env.template` paste
5. Settings → Domain → Generate
6. Dashboard-এ ঢুকে Settings save করো → Add Channel → Connect

## 🔐 প্রতি Platform-এর Setup
- **YouTube:** Google Cloud Console → OAuth Client (Web) → scope: `youtube.upload`
- **TikTok:** developers.tiktok.com → Content Posting API enable → scopes: `user.info.basic`, `video.publish`, `video.upload`
- **Facebook + Instagram:** developers.facebook.com → একই Meta App → FB Login + Instagram product enable. IG Business account FB Page-এর সাথে linked থাকতে হবে।

## 📂 Drive Folder Setup
**Public folder (recommended):**
1. Drive folder → Share → Anyone with link → Viewer
2. Settings-এ Drive API Key দাও
3. প্রতি channel-এ Folder ID paste করো

**Private folder:** Drive OAuth Client ID/Secret/Refresh Token দাও।

## 🐛 Known Limitations
- **TikTok PULL_FROM_URL:** আপনার domain TikTok-এ verify করতে হতে পারে। সমস্যা হলে channel settings-এ "Source method" → `FILE_UPLOAD` করো।
- **Instagram:** Drive folder public না থাকলে কাজ করবে না (IG public URL ছাড়া accept করে না)।
- **TikTok Direct Post:** App "Live" status না পেলে শুধু test users-এর জন্য post হবে।

## 📝 Version
**v12.0** — Multi-platform release.
