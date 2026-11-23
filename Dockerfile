FROM node:20-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY reelgen.js ./reelgen.js

ENV NODE_ENV=production
ENV DATA_DIR=/var/data

EXPOSE 10000
CMD ["node", "reelgen.js"]
