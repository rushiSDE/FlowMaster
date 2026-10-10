const express = require('express');
const Joi = require('joi');

const logger = require('../../../shared/logger');
const { publishEvent } = require('../../../shared/kafka');
const { markEventProcessed, isDuplicateEvent } = require('../../../shared/idempotency');

const creditState = new Map();

function simulateCreditCheck({ workflowId, customerId, decision = 'approved' }) {
  const decisionKey = `${workflowId}:${customerId}:${decision}`;

  if (creditState.has(decisionKey)) {
    return creditState.get(decisionKey);
  }

  const result = {
    workflowId,
    customerId,
    service: 'credit',
    result: decision,
    status: decision === 'approved' ? 'approved' : 'rejected',
    timestamp: new Date().toISOString()
  };

  creditState.set(decisionKey, result);
  return result;
}

function processCreditCommand(message) {
  const payload = typeof message === 'string' ? JSON.parse(message) : message;
  const eventId = payload.eventId || `${payload.workflowId}:credit:${Date.now()}`;

  if (payload.type === 'credit.compensate' || payload.eventType === 'credit.compensate') {
    if (isDuplicateEvent(eventId)) return { ok: false, reason: 'DUPLICATE_EVENT' };
    markEventProcessed(eventId, payload.workflowId);
    logger.info({ workflowId: payload.workflowId }, 'Credit compensation applied');
    return { ok: true, status: 'compensated' };
  }

  if (isDuplicateEvent(eventId)) return { ok: false, reason: 'DUPLICATE_EVENT' };
  markEventProcessed(eventId, payload.workflowId);

  const decision = payload.forceFailure ? 'rejected' : (payload.amount || 0) > 10000 ? 'rejected' : 'approved';
  const result = simulateCreditCheck({ workflowId: payload.workflowId, customerId: payload.customerId, decision });

  const output = {
    eventId,
    workflowId: payload.workflowId,
    stepName: 'credit-check',
    status: result.status,
    service: 'credit',
    reason: result.status === 'rejected' ? 'Credit check rejected' : null,
    payload: { customerId: payload.customerId, amount: payload.amount || 0 }
  };

  publishEvent({
    topic: 'flowmaster.credit.results',
    key: payload.workflowId,
    payload: output,
    correlationId: payload.correlationId
  });

  return { ok: true, result: output };
}

function createApp() {
  const app = express();
  app.use(express.json());

  app.get('/health', (req, res) => {
    res.json({
      status: 'ok',
      service: 'credit',
      timestamp: new Date().toISOString()
    });
  });

  app.get('/ready', (req, res) => {
    res.json({
      status: 'ready',
      service: 'credit',
      dependencies: { kafka: 'configured', redis: 'configured' }
    });
  });

  app.post('/check', (req, res) => {
    const schema = Joi.object({
      workflowId: Joi.string().required(),
      customerId: Joi.string().required(),
      decision: Joi.string().valid('approved', 'rejected').default('approved')
    });

    const { error, value } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({ error: 'VALIDATION_ERROR', details: error.details.map((detail) => detail.message) });
    }

    const result = simulateCreditCheck(value);
    logger.info({ result }, 'Credit check processed');
    return res.status(200).json(result);
  });

  return app;
}

module.exports = {
  createApp,
  simulateCreditCheck,
  creditState,
  processCreditCommand
};
