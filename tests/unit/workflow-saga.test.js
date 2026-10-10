const { loadWorkflowDefinition, validateWorkflowDefinition, createSagaState } = require('../../shared/workflow-definition');

describe('FlowMaster workflow saga', () => {
  it('creates a valid saga state for a known workflow', () => {
    const definition = loadWorkflowDefinition('workflows/kyc-workflow.yaml');
    const saga = createSagaState(definition, { customerId: 'cust-123', amount: 2500 });

    expect(definition.name).toBe('kyc-workflow');
    expect(saga.workflowId).toBeDefined();
    expect(saga.steps).toHaveLength(3);
    expect(saga.steps[0].name).toBe('verification');
    expect(saga.steps[1].name).toBe('credit-check');
    expect(saga.steps[2].name).toBe('approval');
  });

  it('rejects invalid workflow configuration', () => {
    const invalid = {
      name: 'broken',
      version: '1.0',
      steps: [
        { name: 'verification', service: '', event: 'identity.verification.started' }
      ]
    };

    expect(() => validateWorkflowDefinition(invalid)).toThrow(/service/i);
  });
});
