/**
 * AFM_CaseTriageService — deterministic triage for Fleet Operations anomaly Cases.
 *
 * Shared logic behind both Phase-07 surfaces (the auto-triage Record-Triggered
 * Flow and the on-demand Agentforce agent), invoked through AFM_TriageCase.triage.
 * For each Case it:
 *   1. creates investigation Tasks from the non-[Auto] runbook triage steps,
 *   2. escalates fact-computable critical lost-signal cases to the Engineering
 *      Triage queue,
 *   3. creates a customer-notification Task when the case is region-scoped
 *      (AFM_Customer_Region__c != 'All'), and
 *   4. links the relevant runbook reference Files to the Case (dedup-guarded).
 *
 * Deterministic and bulk-safe (List in, one DML per object, partial success).
 * It does NOT write AFM_Severity_Justification__c or the notification body
 * prose — that GenAI narrative is Phase-07 Group C (a GenAiPromptTemplate).
 *
 * Runbook data is AFM_Runbook__mdt (queried via SOQL — getInstance()/getAll()
 * return null for the Triage_Steps__c LongTextArea). Files are matched to a
 * runbook by Title (ContentVersion.Title == AFM_Runbook__mdt.Title__c).
 */
public with sharing class AFM_CaseTriageService {

    @TestVisible
    private static final String ENG_QUEUE_DEVNAME = 'Engineering_Triage';
    @TestVisible
    private static final Decimal CRIT_GAP_MINUTES = 30;
    @TestVisible
    private static final Decimal CRIT_ALTITUDE_FT = 10000;
    private static final String AUTO_MARKER = '[Auto]';

    /** Entry point — triage every Case in caseIds. Safe to call in bulk. */
    public static void triageCases(List<Id> caseIds) {
        if (caseIds == null || caseIds.isEmpty()) {
            return;
        }

        Map<Id, Case> casesById = new Map<Id, Case>([
            SELECT Id, Subject, AFM_Case_Type__c, AFM_Customer_Region__c,
                   AFM_Detection_Facts__c, AFM_Flight_Id__c, AFM_Site_Icao__c,
                   AFM_External_Id__c
            FROM Case
            WHERE Id IN :caseIds
        ]);
        if (casesById.isEmpty()) {
            return;
        }

        Map<String, List<AFM_Runbook__mdt>> runbooksByType = runbooksByCaseType();
        Id engQueueId = queueId(ENG_QUEUE_DEVNAME);
        Map<String, Id> docIdByTitle = runbookDocIdsByTitle();
        Set<String> linkKeys = existingLinkKeys(casesById.keySet());

        List<Task> tasks = new List<Task>();
        List<ContentDocumentLink> links = new List<ContentDocumentLink>();

        for (Case c : casesById.values()) {
            List<AFM_Runbook__mdt> runbooks = runbooksByType.get(c.AFM_Case_Type__c);
            if (runbooks == null) {
                runbooks = new List<AFM_Runbook__mdt>();
            }

            // 1. Investigation Tasks from the non-[Auto] runbook steps.
            for (AFM_Runbook__mdt rb : runbooks) {
                for (String step : manualSteps(rb.Triage_Steps__c)) {
                    tasks.add(investigationTask(c, step));
                }
            }

            // 2. Critical escalation (lost_signal, fact-computable only).
            Task escalation = criticalEscalation(c, engQueueId);
            if (escalation != null) {
                tasks.add(escalation);
            }

            // 3. Customer-notification Task for region-scoped cases.
            if (isRegionScoped(c.AFM_Customer_Region__c)) {
                tasks.add(notificationTask(c));
            }

            // 4. Link runbook reference Files (dedup-guarded across org + batch).
            for (AFM_Runbook__mdt rb : runbooks) {
                Id docId = docIdByTitle.get(rb.Title__c);
                if (docId == null) {
                    continue;
                }
                String key = c.Id + ':' + docId;
                if (linkKeys.contains(key)) {
                    continue;
                }
                linkKeys.add(key);
                links.add(new ContentDocumentLink(
                    LinkedEntityId = c.Id,
                    ContentDocumentId = docId,
                    ShareType = 'V',
                    Visibility = 'AllUsers'
                ));
            }
        }

        // Partial-success DML: one bad row never blocks the rest of the triage.
        if (!tasks.isEmpty()) {
            Database.insert(tasks, false);
        }
        if (!links.isEmpty()) {
            Database.insert(links, false);
        }
    }

    // ---- helpers ----------------------------------------------------------

    /** All runbooks, indexed by each case_type in their comma-separated Case_Types__c. */
    private static Map<String, List<AFM_Runbook__mdt>> runbooksByCaseType() {
        Map<String, List<AFM_Runbook__mdt>> byType = new Map<String, List<AFM_Runbook__mdt>>();
        // SOQL, not getAll() — CMDT getInstance()/getAll() return null for LongTextArea.
        for (AFM_Runbook__mdt rb : [
            SELECT DeveloperName, Title__c, Case_Types__c, Severity_Floor__c,
                   Salesforce_Template_Id__c, Triage_Steps__c
            FROM AFM_Runbook__mdt
        ]) {
            if (String.isBlank(rb.Case_Types__c)) {
                continue;
            }
            for (String ct : rb.Case_Types__c.split(',')) {
                String key = ct.trim();
                if (String.isBlank(key)) {
                    continue;
                }
                if (!byType.containsKey(key)) {
                    byType.put(key, new List<AFM_Runbook__mdt>());
                }
                byType.get(key).add(rb);
            }
        }
        return byType;
    }

    /** Non-[Auto] triage steps, one per non-blank line. */
    private static List<String> manualSteps(String triageSteps) {
        List<String> out = new List<String>();
        if (String.isBlank(triageSteps)) {
            return out;
        }
        for (String line : triageSteps.split('\n')) {
            String step = line.trim();
            if (String.isBlank(step) || step.contains(AUTO_MARKER)) {
                continue;
            }
            out.add(step);
        }
        return out;
    }

    private static Task investigationTask(Case c, String step) {
        return new Task(
            WhatId = c.Id,
            Subject = step.abbreviate(255),
            Description = step + '\n\n'
                + 'Case: ' + c.AFM_External_Id__c + ' (' + c.AFM_Case_Type__c + ')\n'
                + 'Flight: ' + emptyDash(c.AFM_Flight_Id__c)
                + '   Site: ' + emptyDash(c.AFM_Site_Icao__c),
            Priority = 'Normal',
            ActivityDate = Date.today()
        );
    }

    /** lost_signal with gap_minutes > 30 AND last_altitude_ft > 10000 -> eng-queue Task. */
    private static Task criticalEscalation(Case c, Id engQueueId) {
        if (c.AFM_Case_Type__c != 'lost_signal' || String.isBlank(c.AFM_Detection_Facts__c)) {
            return null;
        }
        Map<String, Object> facts;
        try {
            facts = (Map<String, Object>) JSON.deserializeUntyped(c.AFM_Detection_Facts__c);
        } catch (Exception e) {
            return null;
        }
        Decimal gap = asDecimal(facts.get('gap_minutes'));
        Decimal alt = asDecimal(facts.get('last_altitude_ft'));
        if (gap == null || alt == null || gap <= CRIT_GAP_MINUTES || alt <= CRIT_ALTITUDE_FT) {
            return null;
        }
        Task t = new Task(
            WhatId = c.Id,
            Subject = ('CRITICAL ESCALATION: ' + c.Subject).abbreviate(255),
            Description = 'Lost-signal critical escalation — gap ' + gap
                + ' min at ' + alt + ' ft (no recovery).\n\nDetection facts:\n'
                + c.AFM_Detection_Facts__c,
            Priority = 'High',
            ActivityDate = Date.today()
        );
        if (engQueueId != null) {
            t.OwnerId = engQueueId;
        }
        return t;
    }

    private static Task notificationTask(Case c) {
        // Deterministic shell; Group C's GenAiPromptTemplate fills the body with
        // the customer-facing narrative. The subject prefix lets Group C find it.
        // (EmailMessage Draft was rejected: this org has Email-to-Case off and
        // "Enable Email Drafts" is a non-metadata, manual Setup toggle.)
        return new Task(
            WhatId = c.Id,
            Subject = ('Customer notification — ' + c.Subject).abbreviate(255),
            Description = 'Draft customer notification for ' + c.AFM_Case_Type__c
                + ' affecting ' + emptyDash(c.AFM_Site_Icao__c)
                + ' (region: ' + c.AFM_Customer_Region__c + ').\n\n'
                + '[Narrative pending — generated by triage.]',
            Priority = 'Normal',
            ActivityDate = Date.today()
        );
    }

    private static Boolean isRegionScoped(String region) {
        return String.isNotBlank(region) && region != 'All';
    }

    private static Id queueId(String developerName) {
        for (Group g : [
            SELECT Id FROM Group
            WHERE Type = 'Queue' AND DeveloperName = :developerName
            LIMIT 1
        ]) {
            return g.Id;
        }
        return null;
    }

    /** Title -> latest runbook ContentVersion's ContentDocumentId. */
    private static Map<String, Id> runbookDocIdsByTitle() {
        Map<String, Id> byTitle = new Map<String, Id>();
        for (ContentVersion cv : [
            SELECT Title, ContentDocumentId FROM ContentVersion
            WHERE IsLatest = true AND PathOnClient LIKE '%.md'
        ]) {
            byTitle.put(cv.Title, cv.ContentDocumentId);
        }
        return byTitle;
    }

    private static Set<String> existingLinkKeys(Set<Id> caseIds) {
        Set<String> keys = new Set<String>();
        for (ContentDocumentLink cdl : [
            SELECT LinkedEntityId, ContentDocumentId FROM ContentDocumentLink
            WHERE LinkedEntityId IN :caseIds
        ]) {
            keys.add(cdl.LinkedEntityId + ':' + cdl.ContentDocumentId);
        }
        return keys;
    }

    private static Decimal asDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Decimal) {
            return (Decimal) value;
        }
        if (value instanceof Integer) {
            return (Decimal) (Integer) value;
        }
        if (value instanceof Double) {
            return Decimal.valueOf((Double) value);
        }
        try {
            return Decimal.valueOf(String.valueOf(value));
        } catch (Exception e) {
            return null;
        }
    }

    private static String emptyDash(String value) {
        return String.isBlank(value) ? '—' : value;
    }
}
