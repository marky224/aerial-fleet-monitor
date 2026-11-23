/**
 * Tests for AFM_RunbookFileSeeder.
 *
 * Every File is created in the test: Apex tests can't see the org's own Files.
 * The Case link in changedBody_addsVersionAndKeepsCaseLink is made by triage
 * itself (AFM_CaseTriageService.triageCases, the call the trigger handler
 * makes), so that test also proves triage still finds a File once it has a new
 * version. It reads the seeded AFM_Runbook__mdt records, as
 * AFM_CaseTriageService_Test does; the other tests use titles no runbook has.
 */
@IsTest
private class AFM_RunbookFileSeeder_Test {

    static {
        // Triage is called directly where a test needs it, so keep the insert
        // trigger and its narrative job out. The static block runs before each
        // test method (statics reset per method).
        AFM_CaseTriggerHandler.bypass = true;
    }

    private static final String TITLE_A = 'AFM Test Runbook A';
    private static final String TITLE_B = 'AFM Test Runbook B';
    private static final String TITLE_C = 'AFM Test Runbook C';
    private static final String PATH_A = 'afm-test-runbook-a.md';
    private static final String CRUISE_TITLE = 'Lost Signal During Cruise';

    // ---- helpers -----------------------------------------------------------

    private static List<AFM_RunbookFileSeeder.Result> seedOne(String title, String path, Blob body) {
        return AFM_RunbookFileSeeder.seed(new List<AFM_RunbookFileSeeder.RunbookFile>{
            new AFM_RunbookFileSeeder.RunbookFile(title, path, body)
        });
    }

    /** Inserts a File with the given bytes (the factory's File has a fixed body). */
    private static Id insertFile(String title, String path, Blob body) {
        ContentVersion cv = new ContentVersion(
            Title = title, PathOnClient = path, VersionData = body);
        insert cv;
        return [SELECT ContentDocumentId FROM ContentVersion WHERE Id = :cv.Id].ContentDocumentId;
    }

    private static Integer versionCount(Id docId) {
        return [SELECT COUNT() FROM ContentVersion WHERE ContentDocumentId = :docId];
    }

    private static ContentVersion latestVersion(Id docId) {
        return [
            SELECT Title, PathOnClient, VersionData FROM ContentVersion
            WHERE ContentDocumentId = :docId AND IsLatest = true
        ];
    }

    // ---- the three actions -------------------------------------------------

    @IsTest
    static void newRunbook_createsFile() {
        Blob body = Blob.valueOf('# ' + TITLE_A + '\n\nA new runbook.');

        Test.startTest();
        List<AFM_RunbookFileSeeder.Result> res = seedOne(TITLE_A, PATH_A, body);
        Test.stopTest();

        System.assertEquals(1, res.size(), 'One result for the one runbook');
        System.assertEquals(AFM_RunbookFileSeeder.CREATED, res[0].action,
            'No File has the title -> created');
        List<ContentVersion> versions = [
            SELECT Title, PathOnClient, VersionData FROM ContentVersion WHERE Title = :TITLE_A
        ];
        System.assertEquals(1, versions.size(), 'One version of one new File');
        System.assertEquals(PATH_A, versions[0].PathOnClient, 'The path is the one passed in');
        System.assertEquals(body.toString(), versions[0].VersionData.toString(),
            'The File holds the bytes passed in');
        System.assertEquals(0, res[0].caseLinksBefore, 'A new File has no Case link before');
        System.assertEquals(0, res[0].caseLinksAfter, 'A new File has no Case link after');
        System.assertEquals(
            'RUNBOOK_FILE action=created path=afm-test-runbook-a.md'
                + ' case_links_before=0 case_links_after=0 title="AFM Test Runbook A"',
            res[0].line(), 'The exact line the seed script reads');
    }

    @IsTest
    static void changedBody_addsVersionAndKeepsCaseLink() {
        Id docId = AFM_TestDataFactory.insertRunbookFile(CRUISE_TITLE);
        Case linked = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'All', null);
        AFM_CaseTriageService.triageCases(new List<Id>{ linked.Id });
        List<ContentDocumentLink> linksBefore = [
            SELECT Id, ContentDocumentId FROM ContentDocumentLink
            WHERE LinkedEntityId = :linked.Id
        ];
        System.assertEquals(1, linksBefore.size(),
            'Precondition: triage linked the runbook File (run make sf-seed-runbooks)');
        System.assertEquals(docId, linksBefore[0].ContentDocumentId,
            'Precondition: the link is to the runbook File');
        Blob changed = Blob.valueOf('# ' + CRUISE_TITLE + '\n\nA changed body.');

        Test.startTest();
        List<AFM_RunbookFileSeeder.Result> res = seedOne(
            CRUISE_TITLE, 'lost-signal-cruise.md', changed);
        Test.stopTest();

        System.assertEquals(AFM_RunbookFileSeeder.NEW_VERSION, res[0].action,
            'A File with other bytes gets a new version');
        System.assertEquals(2, versionCount(docId), 'The new version is on the same File');
        System.assertEquals(1, [SELECT COUNT() FROM ContentDocument WHERE Title = :CRUISE_TITLE],
            'No second File with the title');
        ContentVersion latest = latestVersion(docId);
        System.assertEquals('lost-signal-cruise.md', latest.PathOnClient,
            'The new version carries the new path');
        System.assertEquals(changed.toString(), latest.VersionData.toString(),
            'The latest version holds the new bytes');

        List<ContentDocumentLink> linksAfter = [
            SELECT Id, ContentDocumentId FROM ContentDocumentLink
            WHERE LinkedEntityId = :linked.Id
        ];
        System.assertEquals(1, linksAfter.size(), 'The Case still has one runbook link');
        System.assertEquals(linksBefore[0].Id, linksAfter[0].Id,
            'The Case keeps the same link row');
        System.assertEquals(1, res[0].caseLinksBefore, 'One Case link counted before');
        System.assertEquals(1, res[0].caseLinksAfter, 'One Case link counted after');

        // A Case triaged after the new version links the same File.
        Case later = AFM_TestDataFactory.insertFleetOpsCase('lost_signal', 'All', null);
        AFM_CaseTriageService.triageCases(new List<Id>{ later.Id });
        List<ContentDocumentLink> laterLinks = [
            SELECT ContentDocumentId FROM ContentDocumentLink WHERE LinkedEntityId = :later.Id
        ];
        System.assertEquals(1, laterLinks.size(), 'Triage links the versioned File to a new Case');
        System.assertEquals(docId, laterLinks[0].ContentDocumentId,
            'The new Case links the same File');
    }

    @IsTest
    static void unchangedBody_addsNothing() {
        Id docId = AFM_TestDataFactory.insertRunbookFile(TITLE_A);
        ContentVersion stored = latestVersion(docId);

        Test.startTest();
        // The File's own bytes under another path: the path is not compared.
        List<AFM_RunbookFileSeeder.Result> res = seedOne(TITLE_A, 'renamed.md', stored.VersionData);
        Test.stopTest();

        System.assertEquals(AFM_RunbookFileSeeder.UNCHANGED, res[0].action,
            'The same bytes -> unchanged');
        System.assertEquals(1, versionCount(docId), 'No version was added');
        System.assertEquals(1, [SELECT COUNT() FROM ContentDocument WHERE Title = :TITLE_A],
            'No second File with the title');
        System.assertEquals(stored.PathOnClient, latestVersion(docId).PathOnClient,
            'The stored path stays');
    }

    @IsTest
    static void bodyDifferingOnlyInBase64Case_addsVersion() {
        // 'abc' and 'abI' encode to YWJj and YWJJ, which a case-insensitive
        // compare (Apex ==) would call equal.
        Id docId = insertFile(TITLE_A, PATH_A, Blob.valueOf('abc'));

        Test.startTest();
        List<AFM_RunbookFileSeeder.Result> res = seedOne(TITLE_A, PATH_A, Blob.valueOf('abI'));
        Test.stopTest();

        System.assertEquals(AFM_RunbookFileSeeder.NEW_VERSION, res[0].action,
            'Bytes whose base64 differs only in letter case are a change');
        System.assertEquals(2, versionCount(docId), 'The new version is on the same File');
        System.assert(latestVersion(docId).VersionData.toString().equals('abI'),
            'The latest version holds the new bytes');
    }

    @IsTest
    static void mixedCall_createsVersionsAndSkips() {
        Id docB = AFM_TestDataFactory.insertRunbookFile(TITLE_B);
        Id docC = AFM_TestDataFactory.insertRunbookFile(TITLE_C);
        Blob sameBytes = latestVersion(docC).VersionData;

        Test.startTest();
        List<AFM_RunbookFileSeeder.Result> res = AFM_RunbookFileSeeder.seed(
            new List<AFM_RunbookFileSeeder.RunbookFile>{
                new AFM_RunbookFileSeeder.RunbookFile(TITLE_A, PATH_A, Blob.valueOf('new')),
                new AFM_RunbookFileSeeder.RunbookFile(
                    TITLE_B, 'afm-test-runbook-b.md', Blob.valueOf('changed')),
                new AFM_RunbookFileSeeder.RunbookFile(TITLE_C, 'afm-test-runbook-c.md', sameBytes)
            });
        Test.stopTest();

        System.assertEquals(3, res.size(), 'One result per runbook');
        System.assertEquals(AFM_RunbookFileSeeder.CREATED, res[0].action, 'First: created');
        System.assertEquals(AFM_RunbookFileSeeder.NEW_VERSION, res[1].action, 'Second: new version');
        System.assertEquals(AFM_RunbookFileSeeder.UNCHANGED, res[2].action, 'Third: unchanged');
        System.assertEquals(1, [SELECT COUNT() FROM ContentVersion WHERE Title = :TITLE_A],
            'The new runbook has one version');
        System.assertEquals(2, versionCount(docB), 'The changed runbook has two versions');
        System.assertEquals(1, versionCount(docC), 'The unchanged runbook has one version');
        System.assertEquals(
            'RUNBOOK_SEED files=3 created=1 new_version=1 unchanged=1'
                + ' case_links_before=0 case_links_after=0',
            AFM_RunbookFileSeeder.summaryLine(res), 'The exact summary line the seed script reads');
    }

    // ---- refusals: nothing is written --------------------------------------

    @IsTest
    static void duplicateTitleInInput_throws() {
        Boolean thrown = false;

        Test.startTest();
        try {
            AFM_RunbookFileSeeder.seed(new List<AFM_RunbookFileSeeder.RunbookFile>{
                new AFM_RunbookFileSeeder.RunbookFile(TITLE_A, PATH_A, Blob.valueOf('one')),
                new AFM_RunbookFileSeeder.RunbookFile(TITLE_A, 'other.md', Blob.valueOf('two'))
            });
        } catch (AFM_RunbookFileSeeder.SeedException e) {
            thrown = true;
            System.assert(e.getMessage().contains(TITLE_A), 'The error names the title');
        }
        Test.stopTest();

        System.assert(thrown, 'Two runbooks with one title fail the seed');
        System.assertEquals(0, [SELECT COUNT() FROM ContentVersion WHERE Title = :TITLE_A],
            'Nothing was written');
    }

    @IsTest
    static void twoFilesWithSameTitle_throws() {
        AFM_TestDataFactory.insertRunbookFile(TITLE_A);
        AFM_TestDataFactory.insertRunbookFile(TITLE_A);
        Boolean thrown = false;

        Test.startTest();
        try {
            seedOne(TITLE_A, PATH_A, Blob.valueOf('changed'));
        } catch (AFM_RunbookFileSeeder.SeedException e) {
            thrown = true;
            System.assert(e.getMessage().contains(TITLE_A), 'The error names the title');
        }
        Test.stopTest();

        System.assert(thrown, 'Two Files with one title fail the seed');
        System.assertEquals(2, [SELECT COUNT() FROM ContentVersion WHERE Title = :TITLE_A],
            'Nothing was written');
    }
}
