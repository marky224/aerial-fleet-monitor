/**
 * AFM_CaseTriggerHandler — Surface 1 entry point (Phase-07 Group C).
 *
 * After insert, for eligible Fleet Operations Cases:
 *   1. runs deterministic triage (AFM_CaseTriageService) synchronously, then
 *   2. enqueues the GenAI narrative (AFM_TriageNarrative) asynchronously.
 *
 * Eligibility mirrors the original spec's Flow entry condition:
 *   RecordType = Fleet_Operations AND AFM_Case_Type__c != null
 *   AND AFM_Severity_Justification__c == null.
 *
 * Set bypass = true in unit tests that exercise triage/narrative in isolation.
 */
public with sharing class AFM_CaseTriggerHandler {

    @TestVisible
    private static Boolean bypass = false;

    private static Id fleetOpsRtId;

    private static Id fleetOpsRecordTypeId() {
        if (fleetOpsRtId == null) {
            fleetOpsRtId = Case.SObjectType.getDescribe()
                .getRecordTypeInfosByDeveloperName().get('Fleet_Operations').getRecordTypeId();
        }
        return fleetOpsRtId;
    }

    public static void afterInsert(List<Case> inserted) {
        if (bypass) {
            return;
        }
        Id rtId = fleetOpsRecordTypeId();
        Set<Id> eligible = new Set<Id>();
        for (Case c : inserted) {
            if (c.RecordTypeId == rtId
                && String.isNotBlank(c.AFM_Case_Type__c)
                && c.AFM_Severity_Justification__c == null) {
                eligible.add(c.Id);
            }
        }
        if (eligible.isEmpty()) {
            return;
        }
        AFM_CaseTriageService.triageCases(new List<Id>(eligible));   // deterministic, synchronous
        AFM_TriageNarrative.enqueue(eligible);                       // GenAI, asynchronous
    }
}
