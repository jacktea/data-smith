#!/bin/bash
# 构建 bin/datasmith。若检测到 web/ 前端与 pnpm 工具链，先构建前端并将产物
# 同步到 internal/server/webfs/dist（go:embed）。工具链缺失时退回占位页面。
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -d web ] && command -v pnpm >/dev/null 2>&1; then
  (cd web && pnpm install --frozen-lockfile && pnpm build)
  rm -rf internal/server/webfs/dist
  mkdir -p internal/server/webfs/dist
  cp -R web/dist/. internal/server/webfs/dist/
fi

go build -ldflags '-s -w' -o bin/datasmith cmd/main.go
