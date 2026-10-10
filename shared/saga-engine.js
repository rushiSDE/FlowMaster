const crypto = require('crypto');

function createSagaState(definition, context = {}) {
  const workflowId = `wf_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;

  return {
    workflowId,
    workflowName: definition.name,
    version: definition.version,
    status: 'pending',
    currentStep: 0,
    customerId: context.customerId || 'anonymous',
    amount: Number(context.amount || 0),
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    steps: definition.steps.map((step, index) => ({
      index,
      name: step.name,
      service: step.service,
      event: step.event,
      status: 'pending',
      retries: 0,
      timeoutMs: step.timeoutMs || 3000,
      compensation: step.compensation || null,
      result: null,
      startedAt: null,
      completedAt: null
    })),
    compensations: [],
    eventHistory: []
  };
}

function normalizeResult(result = {}) {
  return {
    stepName: result.stepName || null,
    status: result.status || 'pending',
    reason: result.reason || null,
    payload: result.payload || {}
  };
}

function advanceSaga(state, result = {}) {
  const normalized = normalizeResult(result);
  const nextState = JSON.parse(JSON.stringify(state));
  const currentIndex = nextState.currentStep;
  const currentStep = nextState.steps[currentIndex];

  if (!currentStep) {
    nextState.status = 'completed';
    nextState.updatedAt = new Date().toISOString();
    return nextState;
  }

  if (normalized.stepName && normalized.stepName !== currentStep.name) {
    nextState.eventHistory.push({
      type: 'out_of_order_event',
      stepName: normalized.stepName,
      status: normalized.status,
      at: new Date().toISOString()
    });
    return nextState;
  }

  currentStep.status = normalized.status === 'success' ? 'success' : 'failed';
  currentStep.result = normalized.payload;
  currentStep.completedAt = new Date().toISOString();
  currentStep.retries = currentStep.retries || 0;

  nextState.eventHistory.push({
    type: `${currentStep.name}.${normalized.status}`,
    stepName: currentStep.name,
    status: normalized.status,
    at: new Date().toISOString(),
    payload: normalized.payload
  });

  if (normalized.status === 'success') {
    if (currentIndex >= nextState.steps.length - 1) {
      nextState.status = 'completed';
    } else {
      nextState.currentStep += 1;
      nextState.steps[nextState.currentStep].status = 'in_progress';
      nextState.status = 'running';
    }
  } else {
    nextState.status = 'failed';
    nextState.failureReason = normalized.reason || `Step ${currentStep.name} failed`;
  }

  nextState.updatedAt = new Date().toISOString();
  return nextState;
}

function applyCompensation(state) {
  const nextState = JSON.parse(JSON.stringify(state));
  nextState.compensations = [];

  nextState.steps.forEach((step) => {
    if (step.status === 'success') {
      step.status = 'compensated';
      if (step.compensation) {
        nextState.compensations.push({
          stepName: step.name,
          service: step.compensation.service,
          event: step.compensation.event,
          status: 'queued'
        });
      }
    }
  });

  nextState.status = 'failed';
  nextState.updatedAt = new Date().toISOString();
  nextState.eventHistory.push({
    type: 'compensation.triggered',
    at: new Date().toISOString(),
    compensations: nextState.compensations
  });

  return nextState;
}

module.exports = {
  createSagaState,
  advanceSaga,
  applyCompensation
};
