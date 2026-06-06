FROM node:20-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN mkdir -p data temp
EXPOSE 3000
CMD ["node", "server.js"]
