const { getPool } = require('./postgres');

const memoryStore = new Map();
const memoryEvents = new Map();

function isDatabaseConfigured() {
  return Boolean(process.env.POSTGRES_HOST || process.env.POSTGRES_DB || process.env.POSTGRES_USER);
}

function createWorkflowRecord(workflow) {
  memoryStore.set(workflow.workflowId, workflow);

  if (!isDatabaseConfigured()) {
    return workflow;
  }

  const pool = getPool();
  return pool.query(
    `INSERT INTO workflows (workflow_id, workflow_name, version, customer_id, amount, status, current_step, payload)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (workflow_id) DO UPDATE SET status = EXCLUDED.status, current_step = EXCLUDED.current_step, payload = EXCLUDED.payload, updated_at = NOW()`,
    [
      workflow.workflowId,
      workflow.workflowName,
      workflow.version,
      workflow.customerId,
      Number(workflow.amount || 0),
      workflow.status,
      workflow.currentStep,
      JSON.stringify(workflow)
    ]
  )
    .then(() => workflow)
    .catch(() => workflow);
}

function getWorkflowRecord(workflowId) {
  if (memoryStore.has(workflowId)) {
    return memoryStore.get(workflowId);
  }

  if (!isDatabaseConfigured()) {
    return null;
  }

  const pool = getPool();
  return pool.query('SELECT * FROM workflows WHERE workflow_id = $1', [workflowId])
    .then((result) => result.rows[0] || null)
    .catch(() => null);
}

function updateWorkflowRecord(workflow) {
  memoryStore.set(workflow.workflowId, workflow);

  if (!isDatabaseConfigured()) {
    return workflow;
  }

  const pool = getPool();
  return pool.query(
    `UPDATE workflows
     SET workflow_name = $2, version = $3, customer_id = $4, amount = $5, status = $6, current_step = $7, payload = $8, updated_at = NOW()
     WHERE workflow_id = $1`,
    [
      workflow.workflowId,
      workflow.workflowName,
      workflow.version,
      workflow.customerId,
      Number(workflow.amount || 0),
      workflow.status,
      workflow.currentStep,
      JSON.stringify(workflow)
    ]
  )
    .then(() => workflow)
    .catch(() => workflow);
}

function appendWorkflowEventRecord(event) {
  const workflowId = event.workflowId;
  const existing = memoryEvents.get(workflowId) || [];
  existing.push(event);
  memoryEvents.set(workflowId, existing);

  if (!isDatabaseConfigured()) {
    return event;
  }

  const pool = getPool();
  return pool.query(
    `INSERT INTO workflow_events (workflow_id, event_id, event_type, step_name, status, payload)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (event_id) DO NOTHING`,
    [
      workflowId,
      event.eventId || `${workflowId}:${Date.now()}`,
      event.type || event.stepName || 'workflow.event',
      event.stepName || null,
      event.status || 'pending',
      JSON.stringify(event.payload || event)
    ]
  )
    .then(() => event)
    .catch(() => event);
}

function listPendingWorkflows() {
  const pending = Array.from(memoryStore.values()).filter((workflow) => ['pending', 'running'].includes(workflow.status));

  if (!isDatabaseConfigured()) {
    return pending;
  }

  const pool = getPool();
  return pool.query("SELECT payload FROM workflows WHERE status IN ('pending','running')")
    .then((result) => result.rows.map((row) => row.payload))
    .catch(() => pending);
}

module.exports = {
  createWorkflowRecord,
  getWorkflowRecord,
  updateWorkflowRecord,
  appendWorkflowEventRecord,
  listPendingWorkflows,
  memoryStore,
  memoryEvents
};
