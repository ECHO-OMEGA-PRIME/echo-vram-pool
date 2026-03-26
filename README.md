# echo-vram-pool

> OpenAI-compatible inference gateway that routes requests through a Cloudflare Tunnel to an on-premise distributed VRAM pool (llama.cpp RPC cluster).

## Overview

A Cloudflare Worker that exposes a public, authenticated OpenAI-compatible API backed by the Echo Prime on-premise GPU cluster. Requests are proxied through a Cloudflare Tunnel to the LAN coordinator, which distributes inference across multiple GPU nodes. The Worker handles API key authentication, response caching for identical prompts, usage tracking in D1, SSE streaming passthrough, and cluster health monitoring.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` or `/health` | Service health check with coordinator reachability status |
| GET | `/docs` or `/api` | API documentation and endpoint listing |
| GET | `/v1/models` | List available models on the cluster |
| POST | `/v1/chat/completions` | OpenAI-compatible chat completion (streaming supported) |
| POST | `/v1/completions` | OpenAI-compatible text completion (streaming supported) |
| POST | `/v1/models/load` | Load a model into the GPU cluster (3-min timeout) |
| POST | `/v1/models/unload` | Unload the current model from the cluster |
| GET | `/v1/cluster/status` | Full cluster status (nodes, VRAM, active model) |
| GET | `/v1/cluster/nodes` | List individual cluster nodes |
| GET | `/v1/cluster/gpu` | GPU utilization details per node |
| GET | `/v1/usage` | Usage statistics (today, weekly, top models) |
| POST | `/init` | Initialize D1 usage and health tables |

## Configuration

### Secrets

- `ECHO_API_KEY` — API key for all authenticated endpoints (via `X-Echo-API-Key` header or `Bearer` token)
- `COORDINATOR_URL` — LAN coordinator URL accessible through Cloudflare Tunnel

### Bindings

| Binding | Type | Resource |
|---------|------|----------|
| `DB` | D1 Database | `echo-vram-pool` (usage logs, cluster health history) |
| `CACHE` | KV Namespace | Response cache for identical prompts (5-min TTL) |

### Environment Variables

- `CLUSTER_NAME` — `ECHO_VRAM_POOL`
- `VRAM_POOL_VERSION` — `1.0.0`

## Deployment

```bash
cd O:\ECHO_OMEGA_PRIME\WORKERS\echo-vram-pool && npx wrangler deploy
```

## Architecture

```
Client (OpenAI SDK / curl)
  --> echo-vram-pool Worker (auth + cache + tracking)
    --> Cloudflare Tunnel
      --> LAN Coordinator (llama.cpp RPC)
        --> GPU Node: ALPHA (RTX 4060 + GTX 1080)
        --> GPU Node: BRAVO (RTX 3070)
```

The Worker authenticates requests, checks KV cache for non-streaming identical prompts, proxies to the on-premise coordinator via Cloudflare Tunnel, and tracks usage in D1. Streaming responses (SSE) are passed through directly. Non-streaming responses are cached in KV for 5 minutes to reduce GPU load for repeated queries. Model loading has a 3-minute timeout to accommodate large model initialization. Inference requests have a 5-minute timeout for long generations.
