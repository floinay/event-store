FROM node:24.18.0-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
COPY apps ./apps
COPY packages ./packages
COPY tools ./tools
COPY tsconfig.json tsconfig.base.json ./
RUN npm ci && npm run build

FROM node:24.18.0-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages ./packages
COPY --from=build /app/tools/migrate/dist ./tools/migrate/dist
COPY --from=build /app/tools/migrate/package.json ./tools/migrate/package.json
COPY --from=build /app/tools/bootstrap/dist ./tools/bootstrap/dist
COPY migrations ./migrations
COPY deploy/connector ./deploy/connector
COPY deploy/kafka ./deploy/kafka
CMD ["node", "tools/bootstrap/dist/index.js"]
