/**
 * Tests for AFM_CaseTriageService (via the AFM_TriageCase invocable entry point).
 *
 * The service is deterministic and LLM-free, so these are organic unit tests —
 * no ConnectApi mock. They run against the real AFM_Runbook__mdt records (custom
 * metadata is visible in test context), so the expected investigation-task counts
 * are derived from the same CMDT the service reads rather than hard-coded — a
 * runbook edit can't silently break the assertions.
 */
@IsTest
private class AFM_CaseTriageService_Test {

    private static final String ESC_PREFIX = 'CRITICAL ESCALATION:';
    private static final String NOTIF_PREFIX = 'Customer notification — ';

    // ---- oracles / helpers -------------------------------------------------

    /** Expected non-[Auto] runbook steps for a case_type, computed from live CMDT. */
    private static List<String> expectedManualSteps(String caseType) {
        List<String> steps = new List<String>();
        for (AFM_Runbook__mdt rb : [
            SELECT Case_Types__c, Triage_Steps__c FROM AFM_Runbook__mdt
        ]) {
            if (String.isBlank(rb.Case_Types__c) || String.isBlank(rb.Triage_Steps__c)) {
                continue;
            }
            Set<String> types = new Set<String>();
            for (String t : rb.Case_Types__c.split(',')) {
                types.add(t.trim());
            }
            if (!types.contains(caseType)) {
                continue;
            }
            for (String line : rb.Triage_Steps__c.split('\n')) {
                String s = line.trim();
                if (String.isBlank(s) || s.contains('[Auto]')) {
                    continue;
                }
                steps.add(s.abbreviate(255));
            }
        }
        return steps;
    }

    private static List<Task> tasksFor(Id caseId) {
        return [
            SELECT Subject, Priority, OwnerId FROM Task WHERE WhatId = :caseId
        ];
    }

    /** High-priority tasks == critical escalations. */
    private static List<Task> escalations(List<Task> all) {
        List<Task> out = new List<Task>();
        for (Task t : all) {
            if (t.Priority == 'High') {
                out.add(t);
            }
        }
        return out;
    }

    private static List<Task> notifications(List<Task> all) {
        List<Task> out = new List<Task>();
        for (Task t : all) {
            if (t.Priority == 'Normal' && t.Subject.startsWith(NOTIF_PREFIX)) {
                out.add(t);
            }
        }
        return out;
    }

    private static List<Task> investigations(List<Task> all) {
        List<Task> out = new List<Task>();
        for (Task t : all) {
            if (t.Priority == 'Normal' && !t.Subject.startsWith(NOTIF_PREFIX)) {
                out.add(t);
            }
        }
        return out;
    }

    // ---- early-exit paths --------------------------------------------------

    @IsTest
    static void nullAndEmptyInput_createsNothing() {
        Test.startTest();
        AFM_CaseTriageService.triageCases(null);
        AFM_CaseTriageService.triageCases(new List<Id>());
        Test.stopTest();
        System.assertEquals(0, [SELECT COUNT() FROM Task], 'Null/empty input creates no tasks');
    }

    @IsTest
    static void unknownCaseId_createsNothing() {
        // Also exercises the AFM_TriageCase invocable wrapper.
        Case c = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'All', null);
        Id ghost = c.Id;
        delete c;

        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ ghost });
        Test.stopTest();

        System.assertEquals(0, [SELECT COUNT() FROM Task WHERE WhatId = :ghost],
            'A case id that resolves to nothing creates no tasks');
    }

    // ---- full happy path ---------------------------------------------------

    @IsTest
    static void lostSignalCritical_fullTriage() {
        Id docCruise = AFM_TestDataFactory.insertRunbookFile('Lost Signal During Cruise');
        Id docEsc = AFM_TestDataFactory.insertRunbookFile('Severity Escalation Criteria');

        List<String> expected = expectedManualSteps('lost_signal');
        System.assert(!expected.isEmpty(),
            'Precondition: AFM_Runbook__mdt must be seeded (run make sf-seed-runbooks)');

        Case c = AFM_TestDataFactory.insertFleetOpsCase(
            'lost_signal', 'East', AFM_TestDataFactory.lostSignalFacts(45, 35000));

        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();

        List<Task> all = tasksFor(c.Id);
        List<Task> inv = investigations(all);

        // One investigation task per non-[Auto] runbook step, verbatim, no leakage.
        System.assertEquals(expected.size(), inv.size(),
            'One investigation task per non-[Auto] runbook step');
        Set<String> gotSubjects = new Set<String>();
        for (Task t : inv) {
            System.assert(!t.Subject.contains('[Auto]'), 'No [Auto] step became a task: ' + t.Subject);
            gotSubjects.add(t.Subject);
        }
        System.assertEquals(new Set<String>(expected), gotSubjects,
            'Investigation task subjects match the runbook manual steps verbatim');

        System.assertEquals(1, escalations(all).size(),
            'gap 45 > 30 and alt 35000 > 10000 -> one critical escalation');
        System.assertEquals(1, notifications(all).size(),
            'Region East (!= All) -> one customer-notification task');
        System.assertEquals(2, [SELECT COUNT() FROM ContentDocumentLink WHERE LinkedEntityId = :c.Id],
            'Both matching runbook reference Files are linked to the case');
    }

    // ---- escalation branch matrix ------------------------------------------

    @IsTest
    static void lostSignalSubCritical_noEscalationNoNotification() {
        // Double-typed facts below threshold; region All.
        Case c = AFM_TestDataFactory.insertFleetOpsCase(
            'lost_signal', 'All', AFM_TestDataFactory.lostSignalFacts(5.5, 5000.5));

        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();

        List<Task> all = tasksFor(c.Id);
        System.assertEquals(expectedManualSteps('lost_signal').size(), investigations(all).size(),
            'Investigation tasks still created');
        System.assertEquals(0, escalations(all).size(), 'Below-threshold gap/alt -> no escalation');
        System.assertEquals(0, notifications(all).size(), 'Region All -> no customer notification');
    }

    @IsTest
    static void lostSignalMalformedFacts_noEscalation() {
        Case c = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'All', 'not-json-at-all');
        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();
        System.assertEquals(0, escalations(tasksFor(c.Id)).size(),
            'Unparseable detection facts never escalate');
    }

    @IsTest
    static void lostSignalBlankFacts_noEscalation_regionNotifies() {
        Case c = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'West', null);
        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();
        List<Task> all = tasksFor(c.Id);
        System.assertEquals(0, escalations(all).size(), 'Blank facts -> no escalation');
        System.assertEquals(1, notifications(all).size(), 'Region West -> one notification');
    }

    @IsTest
    static void factsMissingOrNonNumeric_noEscalation() {
        Case missing = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'All', '{"callsign":"X"}');
        Case nonNumeric = AFM_TestDataFactory.insertFleetOpsCase(
            'lost_signal', 'All', '{"gap_minutes":"abc","last_altitude_ft":"40000"}');

        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ missing.Id, nonNumeric.Id });
        Test.stopTest();

        System.assertEquals(0, escalations(tasksFor(missing.Id)).size(),
            'Missing gap/alt keys -> no escalation');
        System.assertEquals(0, escalations(tasksFor(nonNumeric.Id)).size(),
            'Non-numeric gap/alt -> no escalation');
    }

    @IsTest
    static void criticalFactsAsStrings_escalates() {
        // gap/alt arrive as quoted JSON strings -> string-to-decimal path still escalates.
        String facts = JSON.serialize(new Map<String, Object>{
            'gap_minutes' => '45', 'last_altitude_ft' => '35000'
        });
        Case c = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'All', facts);
        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();
        System.assertEquals(1, escalations(tasksFor(c.Id)).size(),
            'String-typed numeric facts still escalate');
    }

    // ---- non-lost_signal types ---------------------------------------------

    @IsTest
    static void weatherImpact_regionScoped_noEscalation() {
        List<String> expected = expectedManualSteps('weather_impact');
        System.assert(!expected.isEmpty(), 'Precondition: runbooks seeded');

        Case c = AFM_TestDataFactory.insertFleetOpsCase('weather_impact', 'West', null);
        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();

        List<Task> all = tasksFor(c.Id);
        System.assertEquals(expected.size(), investigations(all).size(),
            'Weather-impact investigation tasks from its runbook');
        System.assertEquals(0, escalations(all).size(),
            'Non lost_signal cases never escalate, even region-scoped');
        System.assertEquals(1, notifications(all).size(), 'Region West -> one notification');
    }

    @IsTest
    static void nullCaseType_noRunbookTasks() {
        Case c = AFM_TestDataFactory.insertFleetOpsCase(null, 'East', null);
        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();

        List<Task> all = tasksFor(c.Id);
        System.assertEquals(0, investigations(all).size(),
            'No case type -> no runbook -> no investigation tasks');
        System.assertEquals(0, escalations(all).size(), 'No escalation');
        System.assertEquals(1, notifications(all).size(),
            'Region-scoped case still gets a notification regardless of type');
    }

    // ---- bulk + idempotency ------------------------------------------------

    @IsTest
    static void bulkTriage_multipleCases() {
        Case a = AFM_TestDataFactory.insertFleetOpsCase(
            'lost_signal', 'East', AFM_TestDataFactory.lostSignalFacts(40, 20000));
        Case b = AFM_TestDataFactory.insertFleetOpsCase('weather_impact', 'All', null);
        Case d = AFM_TestDataFactory.insertFleetOpsCase('diversion', 'West', null);

        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ a.Id, b.Id, d.Id });
        Test.stopTest();

        System.assertEquals(1, escalations(tasksFor(a.Id)).size(), 'a (critical lost_signal) escalates');
        System.assertEquals(1, notifications(tasksFor(a.Id)).size(), 'a (East) notifies');
        System.assertEquals(expectedManualSteps('lost_signal').size(),
            investigations(tasksFor(a.Id)).size(), 'a investigation tasks');

        System.assertEquals(0, escalations(tasksFor(b.Id)).size(), 'b (weather) does not escalate');
        System.assertEquals(0, notifications(tasksFor(b.Id)).size(), 'b (All) does not notify');
        System.assertEquals(expectedManualSteps('weather_impact').size(),
            investigations(tasksFor(b.Id)).size(), 'b investigation tasks');

        System.assertEquals(0, escalations(tasksFor(d.Id)).size(), 'd (diversion) does not escalate');
        System.assertEquals(1, notifications(tasksFor(d.Id)).size(), 'd (West) notifies');
        System.assertEquals(expectedManualSteps('diversion').size(),
            investigations(tasksFor(d.Id)).size(), 'd investigation tasks');
    }

    @IsTest
    static void secondTriage_doesNotDuplicateRunbookLinks() {
        AFM_TestDataFactory.insertRunbookFile('Lost Signal During Cruise');
        AFM_TestDataFactory.insertRunbookFile('Severity Escalation Criteria');
        Case c = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'All', null);

        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        AFM_TriageCase.triage(new List<Id>{ c.Id });   // second pass must not re-link
        Test.stopTest();

        System.assertEquals(2, [SELECT COUNT() FROM ContentDocumentLink WHERE LinkedEntityId = :c.Id],
            'Runbook File links are deduped across triage runs');
    }

    // ---- queue-owner integration (guarded; needs the deployed queue) -------

    @IsTest(SeeAllData=true)
    static void escalationAssignsToDeployedEngineeringQueue() {
        List<Group> q = [
            SELECT Id FROM Group WHERE Type = 'Queue' AND DeveloperName = 'Engineering_Triage' LIMIT 1
        ];
        if (q.isEmpty()) {
            return;   // queue not deployed in this org; nothing to assert
        }
        Case c = AFM_TestDataFactory.insertFleetOpsCase(
            'lost_signal', 'All', AFM_TestDataFactory.lostSignalFacts(45, 35000));

        Test.startTest();
        AFM_TriageCase.triage(new List<Id>{ c.Id });
        Test.stopTest();

        List<Task> esc = escalations(tasksFor(c.Id));
        System.assertEquals(1, esc.size(), 'One critical escalation task');
        System.assertEquals(q[0].Id, esc[0].OwnerId,
            'Escalation task is owned by the Engineering Triage queue');
    }
}
