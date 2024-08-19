# Use the official Bun image
# See all versions at https://hub.docker.com/r/oven/bun/tags
FROM oven/bun:alpine AS base
WORKDIR /usr/src/app

# Install dependencies needed for sharp
RUN apk add --no-cache \
  build-base \
  vips-dev \
  python3 \
  py3-pip \
  bash \
  yarn

# Install dependencies into temp directory to cache them and speed up future builds
FROM base AS install
RUN mkdir -p /temp/dev
COPY package.json bun.lockb /temp/dev/
RUN cd /temp/dev && bun install

# Install sharp
RUN bun install sharp
