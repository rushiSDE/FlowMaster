const seenEvents = new Map();

function markEventProcessed(eventId, workflowId) {
  if (!eventId) return false;
  if (seenEvents.has(eventId)) {
    return true;
  }
  seenEvents.set(eventId, { workflowId, processedAt: new Date().toISOString() });
  return false;
}

function isDuplicateEvent(eventId) {
  return Boolean(eventId && seenEvents.has(eventId));
}

function resetIdempotencyStore() {
  seenEvents.clear();
}

module.exports = {
  markEventProcessed,
  isDuplicateEvent,
  resetIdempotencyStore,
  seenEvents
};
