/**
 * AFM_TriageCase — the single invocable both Phase-07 surfaces call.
 *
 *   Surface 1 (operational core): the AFM_Case_OnInsert_Triage Record-Triggered
 *     Flow (async after-save) on high-severity Fleet Operations Cases.
 *   Surface 2 (showcase): the Fleet Anomaly Triage Agentforce agent.
 *
 * Thin wrapper over AFM_CaseTriageService so all triage determinism lives in one
 * place (TARS "thin agent, fat Apex"). The @InvocableMethod label/description is
 * the agent LLM's tool contract — keep it accurate and concise.
 */
public with sharing class AFM_TriageCase {

    @InvocableMethod(
        label='AFM: Triage Anomaly Case'
        description='Runs deterministic triage on Fleet Operations anomaly Cases: creates investigation Tasks from the matching runbook, escalates critical lost-signal cases to the engineering queue, drafts a region-scoped customer notification, and links the runbook reference files. Pass the Case record Id(s).'
        category='Aerial Fleet Monitor'
    )
    public static void triage(List<Id> caseIds) {
        AFM_CaseTriageService.triageCases(caseIds);
    }
}
