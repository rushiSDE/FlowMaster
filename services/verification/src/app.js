const express = require('express');
const Joi = require('joi');

const logger = require('../../../shared/logger');
const { publishEvent } = require('../../../shared/kafka');
const { markEventProcessed, isDuplicateEvent } = require('../../../shared/idempotency');

const verificationState = new Map();

function simulateVerification({ workflowId, customerId, decision = 'approved' }) {
  const decisionKey = `${workflowId}:${customerId}:${decision}`;

  if (verificationState.has(decisionKey)) {
    return verificationState.get(decisionKey);
  }

  const result = {
    workflowId,
    customerId,
    service: 'verification',
    result: decision,
    status: decision === 'approved' ? 'success' : 'failed',
    timestamp: new Date().toISOString()
  };

  verificationState.set(decisionKey, result);
  return result;
}

function processVerificationCommand(message) {
  const payload = typeof message === 'string' ? JSON.parse(message) : message;
  const eventId = payload.eventId || `${payload.workflowId}:verification:${Date.now()}`;

  if (payload.type === 'verification.compensate' || payload.eventType === 'verification.compensate') {
    if (isDuplicateEvent(eventId)) return { ok: false, reason: 'DUPLICATE_EVENT' };
    markEventProcessed(eventId, payload.workflowId);
    logger.info({ workflowId: payload.workflowId }, 'Verification compensation applied');
    return { ok: true, status: 'compensated' };
  }

  if (isDuplicateEvent(eventId)) return { ok: false, reason: 'DUPLICATE_EVENT' };
  markEventProcessed(eventId, payload.workflowId);

  const decision = payload.forceFailure ? 'rejected' : (payload.customerId || '').includes('blocked') ? 'rejected' : 'approved';
  const result = simulateVerification({ workflowId: payload.workflowId, customerId: payload.customerId, decision });

  const output = {
    eventId,
    workflowId: payload.workflowId,
    stepName: 'verification',
    status: result.status,
    service: 'verification',
    reason: result.status === 'failed' ? 'Verification rejected' : null,
    payload: { customerId: payload.customerId, amount: payload.amount || 0 }
  };

  publishEvent({
    topic: 'flowmaster.verification.results',
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
      service: 'verification',
      timestamp: new Date().toISOString()
    });
  });

  app.get('/ready', (req, res) => {
    res.json({
      status: 'ready',
      service: 'verification',
      dependencies: { kafka: 'configured', redis: 'configured' }
    });
  });

  app.post('/verify', (req, res) => {
    const schema = Joi.object({
      workflowId: Joi.string().required(),
      customerId: Joi.string().required(),
      decision: Joi.string().valid('approved', 'rejected').default('approved')
    });

    const { error, value } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({ error: 'VALIDATION_ERROR', details: error.details.map((detail) => detail.message) });
    }

    const result = simulateVerification(value);
    logger.info({ result }, 'Verification processed');
    return res.status(200).json(result);
  });

  return app;
}

module.exports = {
  createApp,
  simulateVerification,
  verificationState,
  processVerificationCommand
};
