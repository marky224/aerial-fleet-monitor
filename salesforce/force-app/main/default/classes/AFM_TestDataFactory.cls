/**
 * AFM_TestDataFactory — shared builders for AFM Apex tests.
 *
 * Keeps Case / runbook-File setup boilerplate out of the test classes. Every
 * Case uses the Fleet_Operations record type and a unique AFM_External_Id__c
 * (the field is a unique external id, so each inserted Case needs a distinct
 * value within the test transaction).
 */
@IsTest
public class AFM_TestDataFactory {

    private static Integer seq = 0;
    private static Id fleetOpsRtId;

    /** Cached Fleet_Operations Case record-type id. */
    public static Id fleetOpsRecordTypeId() {
        if (fleetOpsRtId == null) {
            fleetOpsRtId = Case.SObjectType.getDescribe()
                .getRecordTypeInfosByDeveloperName()
                .get('Fleet_Operations')
                .getRecordTypeId();
        }
        return fleetOpsRtId;
    }

    /** A distinct, obviously-synthetic external id per call (no collision with real cases). */
    public static String nextExternalId() {
        seq++;
        return 'CASE-TST-' + String.valueOf(seq);
    }

    /**
     * Builds (does not insert) a Fleet Operations Case.
     * @param caseType  AFM_Case_Type__c rule id, or null for the no-runbook path.
     * @param region    AFM_Customer_Region__c ('All' | 'East' | 'West').
     * @param facts     AFM_Detection_Facts__c JSON, or null.
     */
    public static Case buildFleetOpsCase(String caseType, String region, String facts) {
        return new Case(
            RecordTypeId = fleetOpsRecordTypeId(),
            Status = 'New',
            Subject = (caseType == null ? 'anomaly' : caseType) + ' at KJFK',
            AFM_Case_Type__c = caseType,
            AFM_Customer_Region__c = region,
            AFM_Detection_Facts__c = facts,
            AFM_External_Id__c = nextExternalId(),
            AFM_Flight_Id__c = 'abc123',
            AFM_Site_Icao__c = 'KJFK'
        );
    }

    /** Inserts and returns a Fleet Operations Case. */
    public static Case insertFleetOpsCase(String caseType, String region, String facts) {
        Case c = buildFleetOpsCase(caseType, region, facts);
        insert c;
        return c;
    }

    /** lost_signal detection-facts JSON at the given gap / altitude (any numeric-ish type). */
    public static String lostSignalFacts(Object gapMinutes, Object lastAltitudeFt) {
        return JSON.serialize(new Map<String, Object>{
            'callsign' => 'TEST123',
            'last_lat' => 40.1,
            'last_lon' => -74.2,
            'last_seen' => '2026-06-07T12:00:00Z',
            'gap_minutes' => gapMinutes,
            'last_altitude_ft' => lastAltitudeFt
        });
    }

    /**
     * Inserts a runbook reference File (ContentVersion -> ContentDocument) whose
     * Title matches an AFM_Runbook__mdt.Title__c, so AFM_CaseTriageService links
     * it by ContentVersion.Title. Returns the new ContentDocumentId.
     */
    public static Id insertRunbookFile(String title) {
        ContentVersion cv = new ContentVersion(
            Title = title,
            PathOnClient = title.replaceAll('[^a-zA-Z0-9]+', '-').toLowerCase() + '.md',
            VersionData = Blob.valueOf('# ' + title)
        );
        insert cv;
        return [SELECT ContentDocumentId FROM ContentVersion WHERE Id = :cv.Id].ContentDocumentId;
    }
}
