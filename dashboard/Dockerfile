# SolarIQ operations dashboard — production build served by a static server.
#
# Built with the repository root as context (matching api/Dockerfile).

FROM node:20-alpine AS build
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
# Baked in at build time (Vite inlines VITE_* vars into the bundle). The demo
# runbook documents why this must be a host-reachable address, not the Docker
# service name "api" — the bundle runs in the evaluator's browser.
ARG VITE_API_BASE_URL=http://localhost:8000
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
# React Router uses client-side routing; unknown paths must fall back to
# index.html instead of nginx's default 404, or a browser refresh on
# /plants/PLANT_01 would break.
COPY dashboard/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
