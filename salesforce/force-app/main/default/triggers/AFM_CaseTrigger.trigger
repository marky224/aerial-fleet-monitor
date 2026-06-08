/**
 * AFM_CaseTrigger — fires Surface 1 auto-triage (Phase-07 Group C).
 * Thin by design: all logic lives in AFM_CaseTriggerHandler.
 */
trigger AFM_CaseTrigger on Case (after insert) {
    AFM_CaseTriggerHandler.afterInsert(Trigger.new);
}
