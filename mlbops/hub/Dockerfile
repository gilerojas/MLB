FROM node:22-bookworm-slim

WORKDIR /app/mlbops/hub
ENV NEXT_TELEMETRY_DISABLED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY mlbops/hub/package.json mlbops/hub/package-lock.json ./
RUN npm ci

COPY mlbops/hub ./
ENV NODE_ENV=production
RUN npm run build

EXPOSE 3000
CMD ["npm", "run", "start", "--", "--hostname", "0.0.0.0", "--port", "3000"]
