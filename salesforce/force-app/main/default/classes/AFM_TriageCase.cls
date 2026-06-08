/**
 * AFM_TriageCase — the consolidated invocable the Phase-07 on-demand Agentforce
 * agent (Surface 2 "Fleet Anomaly Triage") calls. The Surface-1 auto-triage path
 * (AFM_CaseTrigger) calls AFM_CaseTriageService directly; both routes share the
 * same deterministic logic so triage behaviour lives in exactly one place
 * (TARS "thin agent, fat Apex").
 *
 * The agent action takes ONE Case per invocation (a scalar caseId request — the
 * shape Agentforce LLM actions map reliably; an Id collection does not) and
 * returns a per-Case result the agent verbalises. The @InvocableMethod
 * label/description and every @InvocableVariable description ARE the agent's tool
 * contract — keep them accurate and concise.
 */
public with sharing class AFM_TriageCase {

    public class TriageRequest {
        @InvocableVariable(
            label='Case Id'
            description='The 18-character Salesforce record Id of the Fleet Operations anomaly Case to triage.'
            required=true
        )
        public Id caseId;
    }

    public class TriageResult {
        @InvocableVariable(label='Triaged'
            description='True if the Case was found and triaged; false if the Id resolved to no Case.')
        public Boolean triaged = false;

        @InvocableVariable(label='Investigation Task Count'
            description='Number of investigation Tasks created from the matching runbook.')
        public Integer investigationTaskCount = 0;

        @InvocableVariable(label='Escalated To Engineering'
            description='True if this critical lost-signal Case was escalated to the Engineering Triage queue.')
        public Boolean escalatedToEngineering = false;

        @InvocableVariable(label='Customer Notification Drafted'
            description='True if a region-scoped customer-notification Task was created.')
        public Boolean customerNotificationDrafted = false;

        @InvocableVariable(label='Summary'
            description='Human-readable summary of every triage action taken on the Case.')
        public String summary;
    }

    @InvocableMethod(
        label='AFM: Triage Anomaly Case'
        description='Runs deterministic triage on a Fleet Operations anomaly Case: creates investigation Tasks from the matching runbook, escalates critical lost-signal cases to the engineering queue, and drafts a region-scoped customer-notification Task. Returns a summary of what was done.'
        category='Aerial Fleet Monitor'
    )
    public static List<TriageResult> triage(List<TriageRequest> requests) {
        List<Id> caseIds = new List<Id>();
        for (TriageRequest req : requests) {
            caseIds.add(req.caseId);
        }

        Map<Id, AFM_CaseTriageService.TriageSummary> summaries =
            AFM_CaseTriageService.triageCases(caseIds);

        List<TriageResult> results = new List<TriageResult>();
        for (TriageRequest req : requests) {
            results.add(toResult(req.caseId, summaries.get(req.caseId)));
        }
        return results;
    }

    private static TriageResult toResult(Id caseId, AFM_CaseTriageService.TriageSummary s) {
        TriageResult r = new TriageResult();
        if (s == null) {
            r.summary = 'Case ' + caseId + ' was not found or is not eligible for triage.';
            return r;
        }
        r.triaged = true;
        r.investigationTaskCount = s.investigationTasks;
        r.escalatedToEngineering = s.escalated;
        r.customerNotificationDrafted = s.notificationDrafted;
        r.summary = buildSummary(s);
        return r;
    }

    private static String buildSummary(AFM_CaseTriageService.TriageSummary s) {
        List<String> parts = new List<String>();
        parts.add(s.investigationTasks + ' investigation task'
            + (s.investigationTasks == 1 ? '' : 's') + ' created');
        if (s.escalated) {
            parts.add('escalated to the Engineering Triage queue');
        }
        if (s.notificationDrafted) {
            parts.add('customer notification drafted');
        }
        return String.join(parts, '; ') + '.';
    }
}
