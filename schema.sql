-- ═══════════════════════════════════════════════════════════════════════════
-- ECHO OMEGA PRIME — VRAM Pool Manager Schema v1.0
-- D1 Database: echo-vram-pool (8c314e20-3faf-4bc5-bace-5a0d3bedd4d2)
-- Run: npx wrangler d1 execute echo-vram-pool --remote --file=./schema.sql
-- ═══════════════════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS pool_stats;
DROP TABLE IF EXISTS workloads;
DROP TABLE IF EXISTS allocations;
DROP TABLE IF EXISTS nodes;

-- Cluster nodes with GPU inventory
CREATE TABLE nodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  hostname TEXT NOT NULL,
  ip TEXT NOT NULL,
  gpus TEXT NOT NULL DEFAULT '[]',
  total_vram_mb INTEGER NOT NULL DEFAULT 0,
  available_vram_mb INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'offline' CHECK(status IN ('online','offline','maintenance','overloaded')),
  last_heartbeat TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- VRAM allocations per GPU
CREATE TABLE allocations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  node_name TEXT NOT NULL,
  gpu_index INTEGER NOT NULL DEFAULT 0,
  model_name TEXT NOT NULL,
  model_size_mb INTEGER NOT NULL,
  adapter_name TEXT,
  priority INTEGER NOT NULL DEFAULT 5,
  status TEXT NOT NULL DEFAULT 'allocated' CHECK(status IN ('allocated','loading','ready','unloading','freed')),
  allocated_at TEXT NOT NULL DEFAULT (datetime('now')),
  freed_at TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (node_name) REFERENCES nodes(name)
);

-- Workload scheduling queue
CREATE TABLE workloads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  model_name TEXT NOT NULL,
  min_vram_mb INTEGER NOT NULL,
  preferred_node TEXT,
  priority INTEGER NOT NULL DEFAULT 5,
  status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','assigned','running','completed','failed')),
  assigned_node TEXT,
  assigned_gpu INTEGER,
  started_at TEXT,
  completed_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Daily pool utilization statistics
CREATE TABLE pool_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  total_vram_mb INTEGER NOT NULL DEFAULT 0,
  used_vram_mb INTEGER NOT NULL DEFAULT 0,
  utilization REAL NOT NULL DEFAULT 0.0,
  allocations_count INTEGER NOT NULL DEFAULT 0,
  avg_allocation_mb REAL NOT NULL DEFAULT 0.0,
  peak_utilization REAL NOT NULL DEFAULT 0.0,
  UNIQUE(date)
);

-- Indexes for performance
CREATE INDEX idx_nodes_status ON nodes(status);
CREATE INDEX idx_allocations_node ON allocations(node_name);
CREATE INDEX idx_allocations_status ON allocations(status);
CREATE INDEX idx_workloads_status ON workloads(status);
CREATE INDEX idx_pool_stats_date ON pool_stats(date);

-- Seed default cluster nodes
INSERT INTO nodes (name, hostname, ip, gpus, total_vram_mb, available_vram_mb, status)
VALUES (
  'ALPHA',
  'ALPHA-DESKTOP',
  '192.168.1.109',
  '[{"name":"RTX 4060","vram_total_mb":8192,"vram_used_mb":0,"compute_capability":"8.9","temperature":0},{"name":"GTX 1080","vram_total_mb":8192,"vram_used_mb":0,"compute_capability":"6.1","temperature":0}]',
  16384,
  16384,
  'offline'
);

INSERT INTO nodes (name, hostname, ip, gpus, total_vram_mb, available_vram_mb, status)
VALUES (
  'BRAVO',
  'BRAVO-ALIENWARE',
  '192.168.1.11',
  '[{"name":"RTX 3070","vram_total_mb":8192,"vram_used_mb":0,"compute_capability":"8.6","temperature":0}]',
  8192,
  8192,
  'offline'
);

INSERT INTO nodes (name, hostname, ip, gpus, total_vram_mb, available_vram_mb, status)
VALUES (
  'CHARLIE',
  'CHARLIE-KALI',
  '192.168.1.202',
  '[]',
  0,
  0,
  'offline'
);

INSERT INTO nodes (name, hostname, ip, gpus, total_vram_mb, available_vram_mb, status)
VALUES (
  'DELTA',
  'DELTA-OMEN',
  '0.0.0.0',
  '[]',
  0,
  0,
  'offline'
);
