const { createSagaState, advanceSaga, applyCompensation } = require('../../shared/saga-engine');
const { loadWorkflowDefinition } = require('../../shared/workflow-definition');
const { processWorkflowEvent, workflowStore } = require('../../services/orchestrator/src/app');
const { resetIdempotencyStore } = require('../../shared/idempotency');

describe('Saga engine', () => {
  it('moves through the workflow steps on success', () => {
    const definition = loadWorkflowDefinition('workflows/kyc-workflow.yaml');
    const state = createSagaState(definition, { customerId: 'cust-123', amount: 2500 });

    const next = advanceSaga(state, { status: 'success', stepName: 'verification' });
    expect(next.currentStep).toBe(1);
    expect(next.steps[0].status).toBe('success');

    const credit = advanceSaga(next, { status: 'success', stepName: 'credit-check' });
    expect(credit.currentStep).toBe(2);
    expect(credit.status).toBe('running');

    const finalized = advanceSaga(credit, { status: 'success', stepName: 'approval' });
    expect(finalized.status).toBe('completed');
    expect(finalized.steps[2].status).toBe('success');
  });

  it('executes compensation when a credit step is rejected', () => {
    const definition = loadWorkflowDefinition('workflows/kyc-workflow.yaml');
    const state = createSagaState(definition, { customerId: 'cust-123', amount: 2500 });

    const afterVerification = advanceSaga(state, { status: 'success', stepName: 'verification' });
    const failed = advanceSaga(afterVerification, { status: 'rejected', stepName: 'credit-check' });

    expect(failed.status).toBe('failed');
    const compensated = applyCompensation(failed);
    expect(compensated.steps[0].status).toBe('compensated');
    expect(compensated.compensations.length).toBeGreaterThan(0);
  });

  it('ignores duplicate workflow events', () => {
    resetIdempotencyStore();
    const definition = loadWorkflowDefinition('workflows/kyc-workflow.yaml');
    const state = createSagaState(definition, { customerId: 'cust-123', amount: 2500 });
    workflowStore.set(state.workflowId, state);

    const first = processWorkflowEvent(state.workflowId, {
      eventId: 'dup-1',
      stepName: 'verification',
      status: 'success',
      payload: { amount: 2500 }
    });

    const second = processWorkflowEvent(state.workflowId, {
      eventId: 'dup-1',
      stepName: 'verification',
      status: 'success',
      payload: { amount: 2500 }
    });

    expect(first.ok).toBe(true);
    expect(second.ok).toBe(false);
    expect(second.reason).toBe('DUPLICATE_EVENT');
  });
});
