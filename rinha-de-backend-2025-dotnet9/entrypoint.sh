#!/bin/bash
set -e

echo "[Entrypoint] Aguardando o Redis responder com PONG em $REDIS_URL..."

until redis-cli -h "redis" -p 6379 ping | grep -q PONG; do
  echo "[Entrypoint] Redis ainda não está pronto, aguardando..."
  sleep 1
done

echo "[Entrypoint] Redis pronto. Iniciando aplicação .NET..."
exec dotnet rinha-de-backend-2025-dotnet9.dll
