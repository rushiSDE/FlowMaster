CREATE TABLE IF NOT EXISTS workflows (
  workflow_id TEXT PRIMARY KEY,
  workflow_name TEXT NOT NULL,
  version TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  amount NUMERIC(12,2) DEFAULT 0,
  status TEXT NOT NULL,
  current_step INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  payload JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS workflow_steps (
  id SERIAL PRIMARY KEY,
  workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
  step_name TEXT NOT NULL,
  status TEXT NOT NULL,
  service_name TEXT NOT NULL,
  event_name TEXT NOT NULL,
  retries INTEGER DEFAULT 0,
  timeout_ms INTEGER DEFAULT 3000,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (workflow_id, step_name)
);

CREATE TABLE IF NOT EXISTS workflow_events (
  id SERIAL PRIMARY KEY,
  workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
  event_id TEXT NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  step_name TEXT,
  status TEXT NOT NULL,
  payload JSONB DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_messages (
  id SERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  event_id TEXT NOT NULL UNIQUE,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  published_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS idempotency_records (
  event_id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  processed_at TIMESTAMPTZ DEFAULT NOW(),
  result JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow_id ON workflow_events(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow_id ON workflow_steps(workflow_id);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_messages(status);
