FROM node:24-alpine

WORKDIR /app
# corepackはpackageManagerの版のpnpmを使う。pnpm 11はNode 20で動かない
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile --prod
COPY index.js ./

CMD ["node", "index.js"]
