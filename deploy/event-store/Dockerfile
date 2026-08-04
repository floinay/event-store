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
COPY --from=build /app/apps/event-store-service/dist ./apps/event-store-service/dist
COPY --from=build /app/packages ./packages
COPY --from=build /app/tools/migrate/dist ./tools/migrate/dist
COPY migrations ./migrations
CMD ["node", "apps/event-store-service/dist/index.js"]
