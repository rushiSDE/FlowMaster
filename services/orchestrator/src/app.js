const express = require('express');
const Joi = require('joi');

const logger = require('../../../shared/logger');
const { workflowCounter } = require('../../../shared/metrics');
const { initTracing } = require('../../../shared/tracing');
const { loadWorkflowDefinition } = require('../../../shared/workflow-definition');
const { createSagaState, advanceSaga, applyCompensation } = require('../../../shared/saga-engine');
const { publishEvent } = require('../../../shared/kafka');
const { markEventProcessed, isDuplicateEvent } = require('../../../shared/idempotency');
const {
  createWorkflowRecord,
  updateWorkflowRecord,
  appendWorkflowEventRecord,
  listPendingWorkflows
} = require('../../../shared/workflow-repository');

const workflowStore = new Map();
const workflowEvents = new Map();
const outbox = [];

function addWorkflowEvent(workflowId, payload) {
  const existing = workflowEvents.get(workflowId) || [];
  existing.push(payload);
  workflowEvents.set(workflowId, existing);
  appendWorkflowEventRecord({ workflowId, ...payload });
}

function processWorkflowEvent(workflowId, event) {
  const workflow = workflowStore.get(workflowId);
  if (!workflow) return { ok: false, reason: 'WORKFLOW_NOT_FOUND' };

  if (event.eventId && isDuplicateEvent(event.eventId)) {
    return { ok: false, reason: 'DUPLICATE_EVENT' };
  }

  if (event.eventId) {
    markEventProcessed(event.eventId, workflowId);
  }

  const normalized = {
    stepName: event.stepName || event.payload?.stepName || 'unknown',
    status: event.status === 'rejected' ? 'rejected' : event.status === 'failed' ? 'failed' : 'success',
    reason: event.reason || null,
    payload: event.payload || {}
  };

  const nextState = normalized.status === 'success' || normalized.status === 'rejected' || normalized.status === 'failed'
    ? advanceSaga(workflow, normalized)
    : workflow;

  const finalizedState = normalized.status === 'rejected' || normalized.status === 'failed'
    ? applyCompensation(nextState)
    : nextState;

  workflowStore.set(workflowId, finalizedState);
  updateWorkflowRecord(finalizedState);
  addWorkflowEvent(workflowId, {
    eventId: event.eventId || `evt_${Date.now()}`,
    type: `${normalized.stepName}.${normalized.status}`,
    stepName: normalized.stepName,
    status: normalized.status,
    reason: normalized.reason || null,
    payload: normalized.payload,
    workflowId
  });

  return { ok: true, workflow: finalizedState };
}

async function recoverPendingWorkflows() {
  const pending = await listPendingWorkflows();

  for (const workflow of pending) {
    if (!workflow || !workflow.steps || workflow.currentStep >= workflow.steps.length) continue;
    const nextStep = workflow.steps[workflow.currentStep];
    const commandTopic = nextStep.service === 'verification' ? 'flowmaster.verification.commands' : 'flowmaster.credit.commands';

    await publishEvent({
      topic: commandTopic,
      key: workflow.workflowId,
      payload: {
        eventId: `recover_${workflow.workflowId}_${nextStep.name}_${Date.now()}`,
        workflowId: workflow.workflowId,
        stepName: nextStep.name,
        customerId: workflow.customerId,
        amount: workflow.amount,
        type: `${nextStep.name}.started`,
        correlationId: `recover-${workflow.workflowId}`
      },
      correlationId: `recover-${workflow.workflowId}`
    });
  }

  return pending;
}

function createApp() {
  initTracing();

  const app = express();
  app.use(express.json());

  app.use((req, res, next) => {
    const correlationId = req.headers['x-correlation-id'] || `flow-${Date.now()}`;
    req.correlationId = correlationId;
    res.setHeader('x-correlation-id', correlationId);
    next();
  });

  app.get('/health', (req, res) => {
    res.json({
      status: 'ok',
      service: 'orchestrator',
      timestamp: new Date().toISOString()
    });
  });

  app.get('/ready', (req, res) => {
    res.json({
      status: 'ready',
      service: 'orchestrator',
      dependencies: {
        database: 'configured',
        kafka: 'configured',
        redis: 'configured'
      }
    });
  });

  app.get('/metrics', async (req, res) => {
    const { register } = require('../../../shared/metrics');
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  });

  app.post('/workflows', async (req, res) => {
    const workflowSchema = Joi.object({
      customerId: Joi.string().min(3).required(),
      amount: Joi.number().positive().required(),
      workflowName: Joi.string().default('kyc-workflow')
    });

    const { error, value } = workflowSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        error: 'VALIDATION_ERROR',
        details: error.details.map((detail) => detail.message)
      });
    }

    try {
      const definition = loadWorkflowDefinition('workflows/kyc-workflow.yaml');
      const sagaState = createSagaState(definition, {
        customerId: value.customerId,
        amount: value.amount
      });

      workflowStore.set(sagaState.workflowId, sagaState);

      const startedEvent = {
        eventId: `evt_${Date.now()}`,
        workflowId: sagaState.workflowId,
        stepName: 'verification',
        type: 'verification.started',
        status: 'started',
        service: 'orchestrator',
        payload: {
          customerId: value.customerId,
          amount: value.amount
        }
      };

      const stateWithEvent = {
        ...sagaState,
        eventHistory: [
          {
            type: 'verification.started',
            stepName: 'verification',
            status: 'started',
            at: new Date().toISOString(),
            payload: startedEvent.payload
          }
        ]
      };

      workflowStore.set(sagaState.workflowId, stateWithEvent);
      await createWorkflowRecord(stateWithEvent);
      addWorkflowEvent(sagaState.workflowId, startedEvent);
      outbox.push({
        topic: 'flowmaster.verification.commands',
        payload: {
          eventId: startedEvent.eventId,
          workflowId: sagaState.workflowId,
          stepName: 'verification',
          customerId: value.customerId,
          amount: value.amount,
          type: 'verification.started',
          correlationId: req.correlationId
        },
        status: 'pending'
      });

      const kafkaResult = await publishEvent({
        topic: 'flowmaster.verification.commands',
        key: sagaState.workflowId,
        payload: {
          eventId: startedEvent.eventId,
          workflowId: sagaState.workflowId,
          stepName: 'verification',
          customerId: value.customerId,
          amount: value.amount,
          type: 'verification.started',
          correlationId: req.correlationId
        },
        correlationId: req.correlationId
      });

      workflowCounter.inc({ status: 'accepted' });
      logger.info({ workflowId: sagaState.workflowId, customerId: value.customerId, kafkaResult }, 'Workflow accepted');

      return res.status(202).json({
        workflowId: sagaState.workflowId,
        status: stateWithEvent.status,
        currentStep: stateWithEvent.currentStep,
        message: 'Workflow accepted and queued for processing'
      });
    } catch (workflowError) {
      logger.error({ err: workflowError }, 'Unable to create workflow');
      return res.status(400).json({
        error: 'WORKFLOW_DEFINITION_ERROR',
        message: workflowError.message
      });
    }
  });

  app.get('/workflows/:id', (req, res) => {
    const workflow = workflowStore.get(req.params.id);

    if (!workflow) {
      return res.status(404).json({ error: 'WORKFLOW_NOT_FOUND' });
    }

    return res.json({ workflow });
  });

  app.post('/workflows/:id/decision', (req, res) => {
    const workflow = workflowStore.get(req.params.id);
    if (!workflow) {
      return res.status(404).json({ error: 'WORKFLOW_NOT_FOUND' });
    }

    const decisionSchema = Joi.object({
      stepName: Joi.string().required(),
      status: Joi.string().valid('success', 'failed', 'rejected').required(),
      reason: Joi.string().optional(),
      payload: Joi.object().default({})
    });

    const { error, value } = decisionSchema.validate(req.body);
    if (error) {
      return res.status(400).json({ error: 'VALIDATION_ERROR', details: error.details.map((detail) => detail.message) });
    }

    const event = {
      eventId: `evt_${Date.now()}`,
      stepName: value.stepName,
      status: value.status,
      reason: value.reason || null,
      payload: value.payload || {}
    };

    const result = processWorkflowEvent(req.params.id, event);

    return res.json({ workflow: result.workflow || workflow, result: result.ok ? 'processed' : result.reason });
  });

  app.get('/workflows/:id/events', (req, res) => {
    const events = workflowEvents.get(req.params.id) || [];

    return res.json({
      workflowId: req.params.id,
      events
    });
  });

  app.use((error, req, res, next) => {
    logger.error({ err: error, requestId: req.correlationId }, 'Unhandled orchestrator error');
    res.status(500).json({
      error: 'INTERNAL_SERVER_ERROR',
      message: error.message || 'Unexpected error'
    });
  });

  return app;
}

module.exports = {
  createApp,
  workflowStore,
  workflowEvents,
  outbox,
  processWorkflowEvent,
  recoverPendingWorkflows
};
