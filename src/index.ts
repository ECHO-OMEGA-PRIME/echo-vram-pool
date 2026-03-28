/**
 * ═══════════════════════════════════════════════════════════════════════════════
 * ECHO OMEGA PRIME — VRAM Pool Manager v1.0.0
 * ═══════════════════════════════════════════════════════════════════════════════
 * Cloudflare Worker that manages GPU VRAM allocation across the 4-node ECHO
 * cluster. Tracks memory usage, distributes inference workloads, and provides
 * a real-time dashboard for cluster visibility.
 *
 * Nodes: ALPHA (RTX 4060 + GTX 1080), BRAVO (RTX 3070), CHARLIE, DELTA
 * ═══════════════════════════════════════════════════════════════════════════════
 */

// ─── Types ─────────────────────────────────────────────────────────────────────

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  CLUSTER_NAME: string;
  VRAM_POOL_VERSION: string;
  ECHO_API_KEY: string;
  COORDINATOR_URL: string;
}

interface GpuInfo {
  name: string;
  vram_total_mb: number;
  vram_used_mb: number;
  compute_capability: string;
  temperature: number;
}

interface NodeRow {
  id: number;
  name: string;
  hostname: string;
  ip: string;
  gpus: string;
  total_vram_mb: number;
  available_vram_mb: number;
  status: string;
  last_heartbeat: string | null;
  created_at: string;
  updated_at: string;
}

interface AllocationRow {
  id: number;
  node_name: string;
  gpu_index: number;
  model_name: string;
  model_size_mb: number;
  adapter_name: string | null;
  priority: number;
  status: string;
  allocated_at: string;
  freed_at: string | null;
  created_at: string;
}

interface WorkloadRow {
  id: number;
  name: string;
  model_name: string;
  min_vram_mb: number;
  preferred_node: string | null;
  priority: number;
  status: string;
  assigned_node: string | null;
  assigned_gpu: number | null;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  created_at: string;
}

interface PoolStatsRow {
  id: number;
  date: string;
  total_vram_mb: number;
  used_vram_mb: number;
  utilization: number;
  allocations_count: number;
  avg_allocation_mb: number;
  peak_utilization: number;
}

interface AllocateRequest {
  model_name: string;
  size_mb: number;
  priority?: number;
  preferred_node?: string;
  adapter_name?: string;
}

interface FreeRequest {
  allocation_id: number;
}

interface HeartbeatRequest {
  gpus: GpuInfo[];
  status?: string;
}

interface WorkloadRequest {
  name: string;
  model_name: string;
  min_vram_mb: number;
  preferred_node?: string;
  priority?: number;
}

// ─── Structured Logger ─────────────────────────────────────────────────────────

type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'fatal';

function log(level: LogLevel, message: string, data: Record<string, unknown> = {}): void {
  const entry = JSON.stringify({
    ts: new Date().toISOString(),
    level,
    worker: 'echo-vram-pool',
    message,
    ...data,
  });
  if (level === 'error' || level === 'fatal' || level === 'warn') {
    console.error(entry);
  } else {
    console.log(entry);
  }
}

// ─── Constants ─────────────────────────────────────────────────────────────────

const CORS_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Echo-API-Key',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'X-XSS-Protection': '1; mode=block',
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
  'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
  'Referrer-Policy': 'strict-origin-when-cross-origin',
};

const KV_TTL_STATS = 60;       // 1 min cache for stats
const KV_TTL_UTILIZATION = 30; // 30s cache for utilization snapshots
const KV_TTL_NODE_STATUS = 15; // 15s cache for node status

// ─── Response Helpers ──────────────────────────────────────────────────────────

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
  });
}

function htmlResponse(html: string, status = 200): Response {
  return new Response(html, {
    status,
    headers: { 'Content-Type': 'text/html; charset=utf-8', ...CORS_HEADERS },
  });
}

function errorResponse(message: string, status: number, detail?: string): Response {
  return jsonResponse({ error: message, detail: detail ?? undefined, status }, status);
}

// ─── Auth ──────────────────────────────────────────────────────────────────────

function authenticate(request: Request, env: Env): boolean {
  const apiKey =
    request.headers.get('X-Echo-API-Key') ??
    request.headers.get('Authorization')?.replace('Bearer ', '');
  return !!apiKey && apiKey === env.ECHO_API_KEY;
}

// ─── DB Initialization ────────────────────────────────────────────────────────

async function ensureSchema(db: D1Database): Promise<void> {
  await db.batch([
    db.prepare(`CREATE TABLE IF NOT EXISTS nodes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      hostname TEXT NOT NULL,
      ip TEXT NOT NULL,
      gpus TEXT NOT NULL DEFAULT '[]',
      total_vram_mb INTEGER NOT NULL DEFAULT 0,
      available_vram_mb INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'offline',
      last_heartbeat TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS allocations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      node_name TEXT NOT NULL,
      gpu_index INTEGER NOT NULL DEFAULT 0,
      model_name TEXT NOT NULL,
      model_size_mb INTEGER NOT NULL,
      adapter_name TEXT,
      priority INTEGER NOT NULL DEFAULT 5,
      status TEXT NOT NULL DEFAULT 'allocated',
      allocated_at TEXT NOT NULL DEFAULT (datetime('now')),
      freed_at TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS workloads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      model_name TEXT NOT NULL,
      min_vram_mb INTEGER NOT NULL,
      preferred_node TEXT,
      priority INTEGER NOT NULL DEFAULT 5,
      status TEXT NOT NULL DEFAULT 'queued',
      assigned_node TEXT,
      assigned_gpu INTEGER,
      started_at TEXT,
      completed_at TEXT,
      error TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS pool_stats (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      date TEXT NOT NULL,
      total_vram_mb INTEGER NOT NULL DEFAULT 0,
      used_vram_mb INTEGER NOT NULL DEFAULT 0,
      utilization REAL NOT NULL DEFAULT 0.0,
      allocations_count INTEGER NOT NULL DEFAULT 0,
      avg_allocation_mb REAL NOT NULL DEFAULT 0.0,
      peak_utilization REAL NOT NULL DEFAULT 0.0,
      UNIQUE(date)
    )`),
  ]);
}

async function seedDefaultNodes(db: D1Database): Promise<void> {
  const existing = await db.prepare('SELECT COUNT(*) as cnt FROM nodes').first<{ cnt: number }>();
  if (existing && existing.cnt > 0) return;

  const nodes = [
    {
      name: 'ALPHA',
      hostname: 'ALPHA-DESKTOP',
      ip: '192.168.1.109',
      gpus: JSON.stringify([
        { name: 'RTX 4060', vram_total_mb: 8192, vram_used_mb: 0, compute_capability: '8.9', temperature: 0 },
        { name: 'GTX 1080', vram_total_mb: 8192, vram_used_mb: 0, compute_capability: '6.1', temperature: 0 },
      ]),
      total_vram_mb: 16384,
      available_vram_mb: 16384,
    },
    {
      name: 'BRAVO',
      hostname: 'BRAVO-ALIENWARE',
      ip: '192.168.1.11',
      gpus: JSON.stringify([
        { name: 'RTX 3070', vram_total_mb: 8192, vram_used_mb: 0, compute_capability: '8.6', temperature: 0 },
      ]),
      total_vram_mb: 8192,
      available_vram_mb: 8192,
    },
    {
      name: 'CHARLIE',
      hostname: 'CHARLIE-KALI',
      ip: '192.168.1.202',
      gpus: '[]',
      total_vram_mb: 0,
      available_vram_mb: 0,
    },
    {
      name: 'DELTA',
      hostname: 'DELTA-OMEN',
      ip: '0.0.0.0',
      gpus: '[]',
      total_vram_mb: 0,
      available_vram_mb: 0,
    },
  ];

  const stmt = db.prepare(
    `INSERT OR IGNORE INTO nodes (name, hostname, ip, gpus, total_vram_mb, available_vram_mb, status)
     VALUES (?, ?, ?, ?, ?, ?, 'offline')`
  );

  await db.batch(
    nodes.map((n) => stmt.bind(n.name, n.hostname, n.ip, n.gpus, n.total_vram_mb, n.available_vram_mb))
  );

  log('info', 'default_nodes_seeded', { count: nodes.length });
}

// ─── Allocation Algorithm ──────────────────────────────────────────────────────

interface AllocationResult {
  node_name: string;
  gpu_index: number;
  gpu_name: string;
  available_before_mb: number;
  available_after_mb: number;
  allocation_id: number;
}

async function findBestAllocation(
  db: D1Database,
  sizeMb: number,
  preferredNode?: string,
  priority = 5,
  modelName = 'unknown',
  adapterName?: string
): Promise<AllocationResult | null> {
  // Get all online nodes with enough available VRAM
  const candidates = await db
    .prepare(
      `SELECT * FROM nodes WHERE status = 'online' AND available_vram_mb >= ? ORDER BY available_vram_mb DESC`
    )
    .bind(sizeMb)
    .all<NodeRow>();

  if (!candidates.results || candidates.results.length === 0) {
    return null;
  }

  let chosen: NodeRow | null = null;

  // Prefer the requested node if it qualifies
  if (preferredNode) {
    const preferred = candidates.results.find(
      (n) => n.name.toUpperCase() === preferredNode.toUpperCase()
    );
    if (preferred) {
      chosen = preferred;
    }
  }

  // Otherwise pick the node with the most available VRAM (best-fit descending)
  if (!chosen) {
    chosen = candidates.results[0];
  }

  // Find the best GPU on the chosen node
  const gpus: GpuInfo[] = JSON.parse(chosen.gpus);
  let bestGpuIdx = -1;
  let bestFreeVram = -1;

  for (let i = 0; i < gpus.length; i++) {
    const free = gpus[i].vram_total_mb - gpus[i].vram_used_mb;
    if (free >= sizeMb && free > bestFreeVram) {
      bestFreeVram = free;
      bestGpuIdx = i;
    }
  }

  // If no single GPU fits, use the node-level available (multi-GPU split)
  if (bestGpuIdx === -1 && gpus.length > 0) {
    bestGpuIdx = 0; // default to first GPU
  } else if (bestGpuIdx === -1) {
    bestGpuIdx = 0;
  }

  // Create allocation record
  const result = await db
    .prepare(
      `INSERT INTO allocations (node_name, gpu_index, model_name, model_size_mb, adapter_name, priority, status, allocated_at)
       VALUES (?, ?, ?, ?, ?, ?, 'allocated', datetime('now'))
       RETURNING id`
    )
    .bind(chosen.name, bestGpuIdx, modelName, sizeMb, adapterName ?? null, priority)
    .first<{ id: number }>();

  if (!result) return null;

  // Update GPU used VRAM
  if (gpus[bestGpuIdx]) {
    gpus[bestGpuIdx].vram_used_mb += sizeMb;
  }

  const newAvailable = Math.max(0, chosen.available_vram_mb - sizeMb);

  // Update node state
  await db
    .prepare(
      `UPDATE nodes SET gpus = ?, available_vram_mb = ?, updated_at = datetime('now'),
       status = CASE WHEN ? <= 0 THEN 'overloaded' ELSE status END
       WHERE name = ?`
    )
    .bind(JSON.stringify(gpus), newAvailable, newAvailable, chosen.name)
    .run();

  return {
    node_name: chosen.name,
    gpu_index: bestGpuIdx,
    gpu_name: gpus[bestGpuIdx]?.name ?? 'N/A',
    available_before_mb: chosen.available_vram_mb,
    available_after_mb: newAvailable,
    allocation_id: result.id,
  };
}

// ─── KV Cache Helpers ──────────────────────────────────────────────────────────

async function cacheGet<T>(kv: KVNamespace, key: string): Promise<T | null> {
  const val = await kv.get(key, 'json');
  return val as T | null;
}

async function cacheSet(kv: KVNamespace, key: string, data: unknown, ttl: number): Promise<void> {
  await kv.put(key, JSON.stringify(data), { expirationTtl: ttl });
}

// ─── Route Handlers ────────────────────────────────────────────────────────────

async function handleHealth(env: Env): Promise<Response> {
  const startMs = Date.now();

  await ensureSchema(env.DB);
  await seedDefaultNodes(env.DB);

  const nodesResult = await env.DB.prepare(
    `SELECT COUNT(*) as total,
            SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END) as online,
            SUM(total_vram_mb) as total_vram,
            SUM(available_vram_mb) as available_vram
     FROM nodes`
  ).first<{ total: number; online: number; total_vram: number; available_vram: number }>();

  const activeAllocations = await env.DB
    .prepare(`SELECT COUNT(*) as cnt FROM allocations WHERE status IN ('allocated','loading','ready')`)
    .first<{ cnt: number }>();

  const queuedWorkloads = await env.DB
    .prepare(`SELECT COUNT(*) as cnt FROM workloads WHERE status IN ('queued','assigned')`)
    .first<{ cnt: number }>();

  const totalVram = nodesResult?.total_vram ?? 0;
  const availableVram = nodesResult?.available_vram ?? 0;
  const usedVram = totalVram - availableVram;
  const utilization = totalVram > 0 ? ((usedVram / totalVram) * 100) : 0;

  const health = {
    service: 'echo-vram-pool',
    version: env.VRAM_POOL_VERSION,
    cluster: env.CLUSTER_NAME,
    status: 'operational',
    timestamp: new Date().toISOString(),
    latency_ms: Date.now() - startMs,
    pool: {
      total_nodes: nodesResult?.total ?? 0,
      online_nodes: nodesResult?.online ?? 0,
      total_vram_mb: totalVram,
      available_vram_mb: availableVram,
      used_vram_mb: usedVram,
      utilization_pct: Math.round(utilization * 100) / 100,
    },
    active_allocations: activeAllocations?.cnt ?? 0,
    queued_workloads: queuedWorkloads?.cnt ?? 0,
  };

  return jsonResponse(health);
}

async function handleListNodes(env: Env): Promise<Response> {
  await ensureSchema(env.DB);
  await seedDefaultNodes(env.DB);

  const nodes = await env.DB.prepare('SELECT * FROM nodes ORDER BY name').all<NodeRow>();

  const parsed = (nodes.results ?? []).map((n) => ({
    ...n,
    gpus: JSON.parse(n.gpus) as GpuInfo[],
  }));

  return jsonResponse({ nodes: parsed, count: parsed.length });
}

async function handleNodeDetail(env: Env, nodeName: string): Promise<Response> {
  await ensureSchema(env.DB);

  const node = await env.DB
    .prepare('SELECT * FROM nodes WHERE name = ? COLLATE NOCASE')
    .bind(nodeName.toUpperCase())
    .first<NodeRow>();

  if (!node) {
    return errorResponse('Node not found', 404, `No node named "${nodeName}"`);
  }

  const allocations = await env.DB
    .prepare(
      `SELECT * FROM allocations WHERE node_name = ? AND status IN ('allocated','loading','ready')
       ORDER BY priority DESC, allocated_at DESC`
    )
    .bind(node.name)
    .all<AllocationRow>();

  const workloads = await env.DB
    .prepare(
      `SELECT * FROM workloads WHERE assigned_node = ? AND status IN ('assigned','running')
       ORDER BY priority DESC`
    )
    .bind(node.name)
    .all<WorkloadRow>();

  return jsonResponse({
    node: { ...node, gpus: JSON.parse(node.gpus) as GpuInfo[] },
    allocations: allocations.results ?? [],
    workloads: workloads.results ?? [],
  });
}

async function handleHeartbeat(env: Env, nodeName: string, body: HeartbeatRequest): Promise<Response> {
  await ensureSchema(env.DB);
  await seedDefaultNodes(env.DB);

  const node = await env.DB
    .prepare('SELECT * FROM nodes WHERE name = ? COLLATE NOCASE')
    .bind(nodeName.toUpperCase())
    .first<NodeRow>();

  if (!node) {
    return errorResponse('Node not found', 404, `No node named "${nodeName}"`);
  }

  const gpus = body.gpus ?? [];
  const totalVram = gpus.reduce((sum, g) => sum + g.vram_total_mb, 0);
  const usedVram = gpus.reduce((sum, g) => sum + g.vram_used_mb, 0);
  const availableVram = totalVram - usedVram;
  const newStatus = body.status ?? (availableVram <= 0 && totalVram > 0 ? 'overloaded' : 'online');

  await env.DB
    .prepare(
      `UPDATE nodes SET gpus = ?, total_vram_mb = ?, available_vram_mb = ?,
       status = ?, last_heartbeat = datetime('now'), updated_at = datetime('now')
       WHERE name = ?`
    )
    .bind(JSON.stringify(gpus), totalVram, availableVram, newStatus, node.name)
    .run();

  // Cache node status in KV
  await cacheSet(env.CACHE, `node:${node.name}`, {
    name: node.name,
    status: newStatus,
    total_vram_mb: totalVram,
    available_vram_mb: availableVram,
    gpus,
    last_heartbeat: new Date().toISOString(),
  }, KV_TTL_NODE_STATUS);

  log('info', 'node_heartbeat', {
    node: node.name,
    status: newStatus,
    total_vram_mb: totalVram,
    available_vram_mb: availableVram,
    gpu_count: gpus.length,
  });

  return jsonResponse({
    accepted: true,
    node: node.name,
    status: newStatus,
    total_vram_mb: totalVram,
    available_vram_mb: availableVram,
  });
}

async function handleAllocate(env: Env, body: AllocateRequest): Promise<Response> {
  await ensureSchema(env.DB);

  if (!body.model_name || !body.size_mb || body.size_mb <= 0) {
    return errorResponse('Invalid request', 400, 'model_name and size_mb (>0) are required');
  }

  const result = await findBestAllocation(
    env.DB,
    body.size_mb,
    body.preferred_node,
    body.priority ?? 5,
    body.model_name,
    body.adapter_name
  );

  if (!result) {
    // Find what's available for the error message
    const maxAvailable = await env.DB
      .prepare(`SELECT MAX(available_vram_mb) as max_avail FROM nodes WHERE status = 'online'`)
      .first<{ max_avail: number }>();

    log('warn', 'allocation_rejected', {
      model: body.model_name,
      requested_mb: body.size_mb,
      max_available_mb: maxAvailable?.max_avail ?? 0,
    });

    return errorResponse(
      'Insufficient VRAM',
      507,
      `No online node has ${body.size_mb}MB available. Max available: ${maxAvailable?.max_avail ?? 0}MB`
    );
  }

  log('info', 'vram_allocated', {
    allocation_id: result.allocation_id,
    node: result.node_name,
    gpu: result.gpu_index,
    gpu_name: result.gpu_name,
    model: body.model_name,
    size_mb: body.size_mb,
    available_after_mb: result.available_after_mb,
  });

  // Invalidate utilization cache
  await env.CACHE.delete('utilization');

  return jsonResponse({
    allocated: true,
    allocation_id: result.allocation_id,
    node_name: result.node_name,
    gpu_index: result.gpu_index,
    gpu_name: result.gpu_name,
    model_name: body.model_name,
    size_mb: body.size_mb,
    available_before_mb: result.available_before_mb,
    available_after_mb: result.available_after_mb,
  });
}

async function handleFree(env: Env, body: FreeRequest): Promise<Response> {
  await ensureSchema(env.DB);

  if (!body.allocation_id) {
    return errorResponse('Invalid request', 400, 'allocation_id is required');
  }

  const alloc = await env.DB
    .prepare(`SELECT * FROM allocations WHERE id = ? AND status != 'freed'`)
    .bind(body.allocation_id)
    .first<AllocationRow>();

  if (!alloc) {
    return errorResponse('Allocation not found', 404, `No active allocation with id ${body.allocation_id}`);
  }

  // Mark allocation as freed
  await env.DB
    .prepare(`UPDATE allocations SET status = 'freed', freed_at = datetime('now') WHERE id = ?`)
    .bind(alloc.id)
    .run();

  // Restore VRAM to the node
  const node = await env.DB
    .prepare('SELECT * FROM nodes WHERE name = ?')
    .bind(alloc.node_name)
    .first<NodeRow>();

  if (node) {
    const gpus: GpuInfo[] = JSON.parse(node.gpus);
    if (gpus[alloc.gpu_index]) {
      gpus[alloc.gpu_index].vram_used_mb = Math.max(0, gpus[alloc.gpu_index].vram_used_mb - alloc.model_size_mb);
    }

    const newAvailable = Math.min(node.total_vram_mb, node.available_vram_mb + alloc.model_size_mb);
    const newStatus = node.status === 'overloaded' && newAvailable > 0 ? 'online' : node.status;

    await env.DB
      .prepare(
        `UPDATE nodes SET gpus = ?, available_vram_mb = ?, status = ?, updated_at = datetime('now')
         WHERE name = ?`
      )
      .bind(JSON.stringify(gpus), newAvailable, newStatus, node.name)
      .run();
  }

  log('info', 'vram_freed', {
    allocation_id: alloc.id,
    node: alloc.node_name,
    gpu: alloc.gpu_index,
    model: alloc.model_name,
    freed_mb: alloc.model_size_mb,
  });

  // Invalidate utilization cache
  await env.CACHE.delete('utilization');

  return jsonResponse({
    freed: true,
    allocation_id: alloc.id,
    node_name: alloc.node_name,
    gpu_index: alloc.gpu_index,
    model_name: alloc.model_name,
    freed_mb: alloc.model_size_mb,
  });
}

async function handleListAllocations(env: Env): Promise<Response> {
  await ensureSchema(env.DB);

  const active = await env.DB
    .prepare(
      `SELECT * FROM allocations WHERE status IN ('allocated','loading','ready')
       ORDER BY priority DESC, allocated_at DESC`
    )
    .all<AllocationRow>();

  const totalAllocated = (active.results ?? []).reduce((sum, a) => sum + a.model_size_mb, 0);

  return jsonResponse({
    allocations: active.results ?? [],
    count: active.results?.length ?? 0,
    total_allocated_mb: totalAllocated,
  });
}

async function handleListWorkloads(env: Env): Promise<Response> {
  await ensureSchema(env.DB);

  const workloads = await env.DB
    .prepare(
      `SELECT * FROM workloads WHERE status NOT IN ('completed','failed')
       ORDER BY priority DESC, created_at ASC`
    )
    .all<WorkloadRow>();

  return jsonResponse({
    workloads: workloads.results ?? [],
    count: workloads.results?.length ?? 0,
  });
}

async function handleSubmitWorkload(env: Env, body: WorkloadRequest): Promise<Response> {
  await ensureSchema(env.DB);

  if (!body.name || !body.model_name || !body.min_vram_mb || body.min_vram_mb <= 0) {
    return errorResponse('Invalid request', 400, 'name, model_name, and min_vram_mb (>0) are required');
  }

  // Insert the workload
  const result = await env.DB
    .prepare(
      `INSERT INTO workloads (name, model_name, min_vram_mb, preferred_node, priority, status)
       VALUES (?, ?, ?, ?, ?, 'queued') RETURNING id`
    )
    .bind(body.name, body.model_name, body.min_vram_mb, body.preferred_node ?? null, body.priority ?? 5)
    .first<{ id: number }>();

  if (!result) {
    return errorResponse('Failed to create workload', 500);
  }

  // Attempt immediate scheduling
  const allocation = await findBestAllocation(
    env.DB,
    body.min_vram_mb,
    body.preferred_node,
    body.priority ?? 5,
    body.model_name
  );

  if (allocation) {
    await env.DB
      .prepare(
        `UPDATE workloads SET status = 'assigned', assigned_node = ?, assigned_gpu = ?,
         started_at = datetime('now') WHERE id = ?`
      )
      .bind(allocation.node_name, allocation.gpu_index, result.id)
      .run();

    log('info', 'workload_assigned', {
      workload_id: result.id,
      name: body.name,
      node: allocation.node_name,
      gpu: allocation.gpu_index,
    });

    return jsonResponse({
      workload_id: result.id,
      status: 'assigned',
      node_name: allocation.node_name,
      gpu_index: allocation.gpu_index,
      allocation_id: allocation.allocation_id,
    }, 201);
  }

  log('info', 'workload_queued', {
    workload_id: result.id,
    name: body.name,
    model: body.model_name,
    min_vram_mb: body.min_vram_mb,
  });

  return jsonResponse({
    workload_id: result.id,
    status: 'queued',
    message: 'No node currently has sufficient VRAM. Workload queued for later scheduling.',
  }, 202);
}

async function handleStats(env: Env): Promise<Response> {
  await ensureSchema(env.DB);

  // Check KV cache first
  const cached = await cacheGet<Record<string, unknown>>(env.CACHE, 'pool_stats');
  if (cached) {
    return jsonResponse({ ...cached, cached: true });
  }

  const nodes = await env.DB.prepare('SELECT * FROM nodes').all<NodeRow>();
  const nodeList = nodes.results ?? [];

  const totalVram = nodeList.reduce((s, n) => s + n.total_vram_mb, 0);
  const availableVram = nodeList.reduce((s, n) => s + n.available_vram_mb, 0);
  const usedVram = totalVram - availableVram;
  const utilization = totalVram > 0 ? (usedVram / totalVram) * 100 : 0;

  const activeAllocs = await env.DB
    .prepare(`SELECT COUNT(*) as cnt, COALESCE(SUM(model_size_mb), 0) as total_mb, COALESCE(AVG(model_size_mb), 0) as avg_mb
              FROM allocations WHERE status IN ('allocated','loading','ready')`)
    .first<{ cnt: number; total_mb: number; avg_mb: number }>();

  const todayAllocCount = await env.DB
    .prepare(`SELECT COUNT(*) as cnt FROM allocations WHERE DATE(allocated_at) = DATE('now')`)
    .first<{ cnt: number }>();

  const weekAllocCount = await env.DB
    .prepare(`SELECT COUNT(*) as cnt FROM allocations WHERE allocated_at >= datetime('now', '-7 days')`)
    .first<{ cnt: number }>();

  const recentStats = await env.DB
    .prepare(`SELECT * FROM pool_stats ORDER BY date DESC LIMIT 7`)
    .all<PoolStatsRow>();

  const stats = {
    current: {
      total_vram_mb: totalVram,
      used_vram_mb: usedVram,
      available_vram_mb: availableVram,
      utilization_pct: Math.round(utilization * 100) / 100,
      online_nodes: nodeList.filter((n) => n.status === 'online').length,
      total_nodes: nodeList.length,
    },
    active_allocations: {
      count: activeAllocs?.cnt ?? 0,
      total_mb: activeAllocs?.total_mb ?? 0,
      avg_mb: Math.round((activeAllocs?.avg_mb ?? 0) * 100) / 100,
    },
    today: {
      allocations: todayAllocCount?.cnt ?? 0,
    },
    week: {
      allocations: weekAllocCount?.cnt ?? 0,
    },
    history: recentStats.results ?? [],
    timestamp: new Date().toISOString(),
  };

  // Record daily stats
  const today = new Date().toISOString().split('T')[0];
  try {
    await env.DB
      .prepare(
        `INSERT INTO pool_stats (date, total_vram_mb, used_vram_mb, utilization, allocations_count, avg_allocation_mb, peak_utilization)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(date) DO UPDATE SET
           total_vram_mb = excluded.total_vram_mb,
           used_vram_mb = MAX(pool_stats.used_vram_mb, excluded.used_vram_mb),
           utilization = excluded.utilization,
           allocations_count = excluded.allocations_count,
           avg_allocation_mb = excluded.avg_allocation_mb,
           peak_utilization = MAX(pool_stats.peak_utilization, excluded.utilization)`
      )
      .bind(
        today,
        totalVram,
        usedVram,
        Math.round(utilization * 100) / 100,
        activeAllocs?.cnt ?? 0,
        Math.round((activeAllocs?.avg_mb ?? 0) * 100) / 100,
        Math.round(utilization * 100) / 100
      )
      .run();
  } catch {
    // Non-blocking
  }

  await cacheSet(env.CACHE, 'pool_stats', stats, KV_TTL_STATS);

  return jsonResponse(stats);
}

async function handleUtilization(env: Env): Promise<Response> {
  await ensureSchema(env.DB);
  await seedDefaultNodes(env.DB);

  // Check KV cache
  const cached = await cacheGet<Record<string, unknown>>(env.CACHE, 'utilization');
  if (cached) {
    return jsonResponse({ ...cached, cached: true });
  }

  const nodes = await env.DB.prepare('SELECT * FROM nodes ORDER BY name').all<NodeRow>();
  const nodeList = nodes.results ?? [];

  const utilMap = nodeList.map((n) => {
    const gpus: GpuInfo[] = JSON.parse(n.gpus);
    const usedVram = n.total_vram_mb - n.available_vram_mb;
    const utilPct = n.total_vram_mb > 0 ? (usedVram / n.total_vram_mb) * 100 : 0;

    return {
      name: n.name,
      hostname: n.hostname,
      ip: n.ip,
      status: n.status,
      total_vram_mb: n.total_vram_mb,
      used_vram_mb: usedVram,
      available_vram_mb: n.available_vram_mb,
      utilization_pct: Math.round(utilPct * 100) / 100,
      gpus: gpus.map((g) => ({
        name: g.name,
        total_mb: g.vram_total_mb,
        used_mb: g.vram_used_mb,
        free_mb: g.vram_total_mb - g.vram_used_mb,
        utilization_pct:
          g.vram_total_mb > 0
            ? Math.round(((g.vram_used_mb / g.vram_total_mb) * 100) * 100) / 100
            : 0,
        temperature: g.temperature,
      })),
      last_heartbeat: n.last_heartbeat,
    };
  });

  const totalVram = nodeList.reduce((s, n) => s + n.total_vram_mb, 0);
  const totalAvailable = nodeList.reduce((s, n) => s + n.available_vram_mb, 0);
  const totalUsed = totalVram - totalAvailable;
  const clusterUtil = totalVram > 0 ? (totalUsed / totalVram) * 100 : 0;

  const result = {
    cluster: {
      name: env.CLUSTER_NAME,
      total_vram_mb: totalVram,
      used_vram_mb: totalUsed,
      available_vram_mb: totalAvailable,
      utilization_pct: Math.round(clusterUtil * 100) / 100,
      online_nodes: nodeList.filter((n) => n.status === 'online').length,
      total_nodes: nodeList.length,
    },
    nodes: utilMap,
    timestamp: new Date().toISOString(),
  };

  await cacheSet(env.CACHE, 'utilization', result, KV_TTL_UTILIZATION);

  return jsonResponse(result);
}

// ─── Dashboard HTML ────────────────────────────────────────────────────────────

async function handleDashboard(env: Env): Promise<Response> {
  await ensureSchema(env.DB);
  await seedDefaultNodes(env.DB);

  const nodes = await env.DB.prepare('SELECT * FROM nodes ORDER BY name').all<NodeRow>();
  const nodeList = nodes.results ?? [];

  const activeAllocs = await env.DB
    .prepare(
      `SELECT * FROM allocations WHERE status IN ('allocated','loading','ready')
       ORDER BY priority DESC, allocated_at DESC LIMIT 50`
    )
    .all<AllocationRow>();

  const queuedWorkloads = await env.DB
    .prepare(
      `SELECT * FROM workloads WHERE status NOT IN ('completed','failed')
       ORDER BY priority DESC, created_at ASC LIMIT 50`
    )
    .all<WorkloadRow>();

  const totalVram = nodeList.reduce((s, n) => s + n.total_vram_mb, 0);
  const totalAvailable = nodeList.reduce((s, n) => s + n.available_vram_mb, 0);
  const totalUsed = totalVram - totalAvailable;
  const clusterUtil = totalVram > 0 ? (totalUsed / totalVram) * 100 : 0;

  const nodeCards = nodeList.map((n) => {
    const gpus: GpuInfo[] = JSON.parse(n.gpus);
    const usedVram = n.total_vram_mb - n.available_vram_mb;
    const utilPct = n.total_vram_mb > 0 ? (usedVram / n.total_vram_mb) * 100 : 0;

    const statusColor =
      n.status === 'online'
        ? '#22c55e'
        : n.status === 'overloaded'
          ? '#ef4444'
          : n.status === 'maintenance'
            ? '#f59e0b'
            : '#6b7280';

    const barColor =
      utilPct > 90 ? '#ef4444' : utilPct > 70 ? '#f59e0b' : '#22c55e';

    const gpuRows = gpus
      .map((g, i) => {
        const gFree = g.vram_total_mb - g.vram_used_mb;
        const gUtil = g.vram_total_mb > 0 ? (g.vram_used_mb / g.vram_total_mb) * 100 : 0;
        const gBarColor = gUtil > 90 ? '#ef4444' : gUtil > 70 ? '#f59e0b' : '#22c55e';
        return `
          <div style="margin:8px 0;padding:8px;background:#1a1a2e;border-radius:6px;">
            <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:4px;">
              <span>GPU ${i}: ${escapeHtml(g.name)}</span>
              <span>${g.vram_used_mb}/${g.vram_total_mb} MB (${Math.round(gUtil)}%)</span>
            </div>
            <div style="height:8px;background:#2d2d44;border-radius:4px;overflow:hidden;">
              <div style="height:100%;width:${Math.min(gUtil, 100)}%;background:${gBarColor};border-radius:4px;transition:width 0.5s;"></div>
            </div>
            ${g.temperature > 0 ? `<div style="font-size:11px;color:#9ca3af;margin-top:2px;">Temp: ${g.temperature}&deg;C | Compute: ${g.compute_capability}</div>` : `<div style="font-size:11px;color:#9ca3af;margin-top:2px;">Compute: ${g.compute_capability}</div>`}
          </div>`;
      })
      .join('');

    const noGpu = gpus.length === 0
      ? `<div style="padding:12px;color:#6b7280;font-style:italic;text-align:center;">No GPU installed</div>`
      : '';

    return `
      <div style="background:#16162a;border:1px solid #2d2d44;border-radius:12px;padding:20px;min-width:280px;flex:1;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
          <h3 style="margin:0;font-size:18px;color:#e2e8f0;">${escapeHtml(n.name)}</h3>
          <span style="background:${statusColor};color:#000;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600;text-transform:uppercase;">${escapeHtml(n.status)}</span>
        </div>
        <div style="font-size:12px;color:#9ca3af;margin-bottom:8px;">${escapeHtml(n.hostname)} | ${escapeHtml(n.ip)}</div>
        <div style="display:flex;justify-content:space-between;font-size:14px;margin-bottom:6px;">
          <span>VRAM</span>
          <span style="font-weight:600;">${usedVram} / ${n.total_vram_mb} MB</span>
        </div>
        <div style="height:12px;background:#2d2d44;border-radius:6px;overflow:hidden;margin-bottom:12px;">
          <div style="height:100%;width:${Math.min(utilPct, 100)}%;background:${barColor};border-radius:6px;transition:width 0.5s;"></div>
        </div>
        <div style="font-size:13px;color:#9ca3af;margin-bottom:4px;">Available: ${n.available_vram_mb} MB | Util: ${Math.round(utilPct)}%</div>
        ${n.last_heartbeat ? `<div style="font-size:11px;color:#6b7280;">Last heartbeat: ${escapeHtml(n.last_heartbeat)}</div>` : '<div style="font-size:11px;color:#6b7280;">No heartbeat received</div>'}
        <div style="margin-top:12px;border-top:1px solid #2d2d44;padding-top:8px;">
          <div style="font-size:13px;color:#a5b4fc;font-weight:600;margin-bottom:4px;">GPUs</div>
          ${gpuRows}${noGpu}
        </div>
      </div>`;
  }).join('');

  const allocRows = (activeAllocs.results ?? [])
    .map(
      (a) => `
      <tr>
        <td style="padding:8px 12px;">${a.id}</td>
        <td style="padding:8px 12px;">${escapeHtml(a.node_name)}</td>
        <td style="padding:8px 12px;">${a.gpu_index}</td>
        <td style="padding:8px 12px;">${escapeHtml(a.model_name)}</td>
        <td style="padding:8px 12px;">${a.model_size_mb} MB</td>
        <td style="padding:8px 12px;">${a.adapter_name ? escapeHtml(a.adapter_name) : '-'}</td>
        <td style="padding:8px 12px;"><span style="color:${a.status === 'ready' ? '#22c55e' : a.status === 'loading' ? '#f59e0b' : '#60a5fa'}">${escapeHtml(a.status)}</span></td>
        <td style="padding:8px 12px;font-size:12px;">${escapeHtml(a.allocated_at)}</td>
      </tr>`
    )
    .join('');

  const workloadRows = (queuedWorkloads.results ?? [])
    .map(
      (w) => `
      <tr>
        <td style="padding:8px 12px;">${w.id}</td>
        <td style="padding:8px 12px;">${escapeHtml(w.name)}</td>
        <td style="padding:8px 12px;">${escapeHtml(w.model_name)}</td>
        <td style="padding:8px 12px;">${w.min_vram_mb} MB</td>
        <td style="padding:8px 12px;">${w.priority}</td>
        <td style="padding:8px 12px;"><span style="color:${w.status === 'running' ? '#22c55e' : w.status === 'assigned' ? '#60a5fa' : '#f59e0b'}">${escapeHtml(w.status)}</span></td>
        <td style="padding:8px 12px;">${w.assigned_node ? escapeHtml(w.assigned_node) : '-'}</td>
        <td style="padding:8px 12px;font-size:12px;">${escapeHtml(w.created_at)}</td>
      </tr>`
    )
    .join('');

  const clusterBarColor = clusterUtil > 90 ? '#ef4444' : clusterUtil > 70 ? '#f59e0b' : '#22c55e';

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ECHO VRAM Pool | Cluster Dashboard</title>
  <meta http-equiv="refresh" content="30">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a1a; color: #e2e8f0; min-height: 100vh; }
    a { color: #60a5fa; text-decoration: none; }
    a:hover { text-decoration: underline; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th { text-align: left; padding: 10px 12px; background: #1a1a2e; color: #a5b4fc; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid #2d2d44; }
    td { border-bottom: 1px solid #1a1a2e; }
    tr:hover td { background: #16162a; }
    .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px; padding-bottom: 16px; border-bottom: 1px solid #2d2d44; }
    .header h1 { font-size: 24px; color: #a5b4fc; }
    .header .meta { font-size: 13px; color: #6b7280; }
    .cluster-bar { margin-bottom: 32px; padding: 20px; background: #16162a; border: 1px solid #2d2d44; border-radius: 12px; }
    .cluster-bar .stats { display: flex; gap: 32px; margin-bottom: 12px; flex-wrap: wrap; }
    .cluster-bar .stat { text-align: center; }
    .cluster-bar .stat .val { font-size: 28px; font-weight: 700; color: #e2e8f0; }
    .cluster-bar .stat .label { font-size: 12px; color: #6b7280; text-transform: uppercase; }
    .nodes-grid { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 32px; }
    .section { margin-bottom: 32px; }
    .section h2 { font-size: 18px; color: #a5b4fc; margin-bottom: 16px; }
    .table-wrap { overflow-x: auto; background: #0f0f23; border: 1px solid #2d2d44; border-radius: 12px; }
    .empty { padding: 24px; text-align: center; color: #6b7280; font-style: italic; }
    .api-links { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 16px; }
    .api-links a { padding: 6px 14px; background: #1a1a2e; border: 1px solid #2d2d44; border-radius: 8px; font-size: 13px; transition: border-color 0.2s; }
    .api-links a:hover { border-color: #60a5fa; text-decoration: none; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h1>ECHO VRAM Pool</h1>
        <div style="color:#6b7280;font-size:13px;margin-top:4px;">Distributed GPU Memory Manager | ${escapeHtml(env.CLUSTER_NAME)} v${escapeHtml(env.VRAM_POOL_VERSION)}</div>
      </div>
      <div class="meta">
        <div>Auto-refresh: 30s</div>
        <div>${new Date().toISOString()}</div>
      </div>
    </div>

    <div class="cluster-bar">
      <div class="stats">
        <div class="stat"><div class="val">${formatMB(totalVram)}</div><div class="label">Total VRAM</div></div>
        <div class="stat"><div class="val" style="color:${clusterBarColor}">${formatMB(totalUsed)}</div><div class="label">Used</div></div>
        <div class="stat"><div class="val" style="color:#22c55e">${formatMB(totalAvailable)}</div><div class="label">Available</div></div>
        <div class="stat"><div class="val" style="color:${clusterBarColor}">${Math.round(clusterUtil)}%</div><div class="label">Utilization</div></div>
        <div class="stat"><div class="val">${nodeList.filter((n) => n.status === 'online').length}/${nodeList.length}</div><div class="label">Nodes Online</div></div>
        <div class="stat"><div class="val">${(activeAllocs.results ?? []).length}</div><div class="label">Active Allocs</div></div>
        <div class="stat"><div class="val">${(queuedWorkloads.results ?? []).length}</div><div class="label">Workloads</div></div>
      </div>
      <div style="height:16px;background:#2d2d44;border-radius:8px;overflow:hidden;">
        <div style="height:100%;width:${Math.min(clusterUtil, 100)}%;background:${clusterBarColor};border-radius:8px;transition:width 0.5s;"></div>
      </div>
    </div>

    <div class="section">
      <h2>Cluster Nodes</h2>
      <div class="nodes-grid">${nodeCards}</div>
    </div>

    <div class="section">
      <h2>Active Allocations (${(activeAllocs.results ?? []).length})</h2>
      <div class="table-wrap">
        ${(activeAllocs.results ?? []).length > 0 ? `
        <table>
          <thead><tr><th>ID</th><th>Node</th><th>GPU</th><th>Model</th><th>Size</th><th>Adapter</th><th>Status</th><th>Allocated</th></tr></thead>
          <tbody>${allocRows}</tbody>
        </table>` : '<div class="empty">No active allocations</div>'}
      </div>
    </div>

    <div class="section">
      <h2>Workload Queue (${(queuedWorkloads.results ?? []).length})</h2>
      <div class="table-wrap">
        ${(queuedWorkloads.results ?? []).length > 0 ? `
        <table>
          <thead><tr><th>ID</th><th>Name</th><th>Model</th><th>Min VRAM</th><th>Priority</th><th>Status</th><th>Node</th><th>Created</th></tr></thead>
          <tbody>${workloadRows}</tbody>
        </table>` : '<div class="empty">No active workloads</div>'}
      </div>
    </div>

    <div class="section">
      <h2>API Endpoints</h2>
      <div class="api-links">
        <a href="/health">GET /health</a>
        <a href="/nodes">GET /nodes</a>
        <a href="/allocations">GET /allocations</a>
        <a href="/workloads">GET /workloads</a>
        <a href="/stats">GET /stats</a>
        <a href="/utilization">GET /utilization</a>
      </div>
      <div style="margin-top:12px;font-size:12px;color:#6b7280;">
        POST endpoints require <code style="background:#1a1a2e;padding:2px 6px;border-radius:4px;">X-Echo-API-Key</code> header:
        POST /allocate | POST /free | POST /workloads | POST /nodes/:name/heartbeat
      </div>
    </div>
  </div>
</body>
</html>`;

  return htmlResponse(html);
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function formatMB(mb: number): string {
  if (mb >= 1024) {
    return `${(mb / 1024).toFixed(1)} GB`;
  }
  return `${mb} MB`;
}

// ─── Route Matching ────────────────────────────────────────────────────────────

function matchRoute(
  method: string,
  path: string
): { handler: string; params: Record<string, string> } | null {
  // Static routes
  const staticRoutes: Record<string, string> = {
    'GET:/': 'dashboard',
    'GET:/health': 'health',
    'GET:/nodes': 'listNodes',
    'GET:/allocations': 'listAllocations',
    'GET:/workloads': 'listWorkloads',
    'POST:/workloads': 'submitWorkload',
    'POST:/allocate': 'allocate',
    'POST:/free': 'free',
    'GET:/stats': 'stats',
    'GET:/utilization': 'utilization',
  };

  const key = `${method}:${path}`;
  if (staticRoutes[key]) {
    return { handler: staticRoutes[key], params: {} };
  }

  // Dynamic routes
  const nodeDetailMatch = path.match(/^\/nodes\/([^/]+)$/);
  if (nodeDetailMatch && method === 'GET') {
    return { handler: 'nodeDetail', params: { name: nodeDetailMatch[1] } };
  }

  const heartbeatMatch = path.match(/^\/nodes\/([^/]+)\/heartbeat$/);
  if (heartbeatMatch && method === 'POST') {
    return { handler: 'heartbeat', params: { name: heartbeatMatch[1] } };
  }

  return null;
}

// ─── Main Export ───────────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const startMs = Date.now();
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';
    const method = request.method;

    // CORS preflight
    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // Payload size limit (1MB)
    const contentLength = parseInt(request.headers.get('Content-Length') ?? '0', 10);
    if ((method === 'POST' || method === 'PUT') && contentLength > 1_048_576) {
      return errorResponse('Payload too large', 413, 'Maximum payload size is 1MB');
    }

    const route = matchRoute(method, path);

    if (!route) {
      return errorResponse('Not found', 404, `${method} ${path} is not a valid endpoint`);
    }

    // Public endpoints (no auth required)
    const publicHandlers = new Set(['dashboard', 'health']);

    if (!publicHandlers.has(route.handler)) {
      if (!authenticate(request, env)) {
        return errorResponse('Unauthorized', 401, 'Provide X-Echo-API-Key header or Bearer token');
      }
    }

    try {
      let response: Response;

      switch (route.handler) {
        case 'dashboard':
          response = await handleDashboard(env);
          break;
        case 'health':
          response = await handleHealth(env);
          break;
        case 'listNodes':
          response = await handleListNodes(env);
          break;
        case 'nodeDetail':
          response = await handleNodeDetail(env, route.params.name);
          break;
        case 'heartbeat': {
          const heartbeatBody = await request.json() as HeartbeatRequest;
          response = await handleHeartbeat(env, route.params.name, heartbeatBody);
          break;
        }
        case 'allocate': {
          const allocBody = await request.json() as AllocateRequest;
          response = await handleAllocate(env, allocBody);
          break;
        }
        case 'free': {
          const freeBody = await request.json() as FreeRequest;
          response = await handleFree(env, freeBody);
          break;
        }
        case 'listAllocations':
          response = await handleListAllocations(env);
          break;
        case 'listWorkloads':
          response = await handleListWorkloads(env);
          break;
        case 'submitWorkload': {
          const wlBody = await request.json() as WorkloadRequest;
          response = await handleSubmitWorkload(env, wlBody);
          break;
        }
        case 'stats':
          response = await handleStats(env);
          break;
        case 'utilization':
          response = await handleUtilization(env);
          break;
        default:
          response = errorResponse('Not found', 404);
      }

      const latencyMs = Date.now() - startMs;
      log('info', 'request_handled', {
        method,
        path,
        handler: route.handler,
        status: response.status,
        latency_ms: latencyMs,
      });

      return response;
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      const errorStack = err instanceof Error ? err.stack : undefined;

      log('error', 'unhandled_error', {
        method,
        path,
        handler: route.handler,
        error: errorMessage,
        stack: errorStack,
      });

      return errorResponse('Internal server error', 500, errorMessage);
    }
  },
};
