/**
 * AFM_RunbookFileSeeder — seeds the runbook reference Files in one transaction.
 *
 * scripts/seed_runbook_files.sh calls seed() once, from anonymous Apex, with
 * every runbooks/*.md. For each runbook it does exactly one thing:
 *   - created:     no File has the runbook's title, so a new File is inserted;
 *   - new_version: the File's latest version holds other bytes, so a new
 *                  version is added to the same File (Case links stay);
 *   - unchanged:   the latest version already holds these bytes, so nothing
 *                  is written.
 *
 * It never deletes a File and never inserts or deletes a ContentDocumentLink.
 * Its one insert is all-or-nothing and nothing is caught, so any error leaves
 * every File as it was.
 *
 * A runbook's File is found the way AFM_CaseTriageService finds it: the latest
 * ContentVersion whose PathOnClient ends in .md, by Title.
 *
 * Output, as INFO debug lines the seed script reads: one RUNBOOK_FILE line per
 * runbook in input order, then one RUNBOOK_SEED line. A Case link is a
 * ContentDocumentLink whose linked record is a Case.
 */
public with sharing class AFM_RunbookFileSeeder {

    public static final String CREATED = 'created';
    public static final String NEW_VERSION = 'new_version';
    public static final String UNCHANGED = 'unchanged';

    public class SeedException extends Exception {}

    /** One runbook to seed: its File title, source path and bytes. */
    public class RunbookFile {
        public String title;
        public String pathOnClient;
        public Blob body;

        public RunbookFile(String title, String pathOnClient, Blob body) {
            this.title = title;
            this.pathOnClient = pathOnClient;
            this.body = body;
        }
    }

    /** What seed() did for one runbook. */
    public class Result {
        public String title;
        public String pathOnClient;
        public String action;
        public Integer caseLinksBefore = 0;
        public Integer caseLinksAfter = 0;

        public String line() {
            return 'RUNBOOK_FILE action=' + action + ' path=' + pathOnClient
                + ' case_links_before=' + caseLinksBefore
                + ' case_links_after=' + caseLinksAfter
                + ' title="' + title + '"';
        }
    }

    /**
     * Seeds every runbook in files and returns one Result per runbook, in
     * input order. Throws SeedException, before writing anything, when two
     * runbooks or two Files share a title.
     */
    public static List<Result> seed(List<RunbookFile> files) {
        Set<String> titles = new Set<String>();
        for (RunbookFile f : files) {
            if (!titles.add(f.title)) {
                throw new SeedException('Two runbooks share the title "' + f.title + '".');
            }
        }

        // The runbook Files as triage finds them.
        Map<String, ContentVersion> latestByTitle = new Map<String, ContentVersion>();
        for (ContentVersion cv : [
            SELECT Title, ContentDocumentId, VersionData FROM ContentVersion
            WHERE IsLatest = true AND PathOnClient LIKE '%.md' AND Title IN :titles
        ]) {
            if (latestByTitle.containsKey(cv.Title)) {
                throw new SeedException(
                    'Two Files are titled "' + cv.Title + '". Retire one by hand.');
            }
            latestByTitle.put(cv.Title, cv);
        }

        List<Result> results = new List<Result>();
        List<Id> docIds = new List<Id>();                  // parallel to results
        List<ContentVersion> toInsert = new List<ContentVersion>();
        List<Integer> resultIndex = new List<Integer>();   // parallel to toInsert
        for (RunbookFile f : files) {
            ContentVersion latest = latestByTitle.get(f.title);
            Result r = new Result();
            r.title = f.title;
            r.pathOnClient = f.pathOnClient;
            Id docId = null;
            if (latest != null) {
                docId = latest.ContentDocumentId;
            }
            // equals(), not ==: == ignores letter case and base64 does not.
            if (latest != null && EncodingUtil.base64Encode(latest.VersionData)
                    .equals(EncodingUtil.base64Encode(f.body))) {
                r.action = UNCHANGED;
            } else {
                ContentVersion v = new ContentVersion(
                    Title = f.title, PathOnClient = f.pathOnClient, VersionData = f.body);
                if (latest == null) {
                    r.action = CREATED;
                } else {
                    r.action = NEW_VERSION;
                    v.ContentDocumentId = docId;   // a new version of the same File
                }
                toInsert.add(v);
                resultIndex.add(results.size());
            }
            results.add(r);
            docIds.add(docId);
        }

        Map<Id, Integer> linksBefore = caseLinkCounts(docIds);

        insert toInsert;   // the seed's only DML: every row or none

        Map<Id, ContentVersion> saved = new Map<Id, ContentVersion>([
            SELECT Id, ContentDocumentId FROM ContentVersion WHERE Id IN :toInsert
        ]);
        for (Integer i = 0; i < toInsert.size(); i++) {
            docIds[resultIndex[i]] = saved.get(toInsert[i].Id).ContentDocumentId;
        }

        Map<Id, Integer> linksAfter = caseLinkCounts(docIds);
        for (Integer i = 0; i < results.size(); i++) {
            Result r = results[i];
            if (linksBefore.containsKey(docIds[i])) {
                r.caseLinksBefore = linksBefore.get(docIds[i]);
            }
            if (linksAfter.containsKey(docIds[i])) {
                r.caseLinksAfter = linksAfter.get(docIds[i]);
            }
            System.debug(LoggingLevel.INFO, r.line());
        }
        System.debug(LoggingLevel.INFO, summaryLine(results));
        return results;
    }

    /** The RUNBOOK_SEED line: a count per action and both Case-link totals. */
    public static String summaryLine(List<Result> results) {
        Integer createdCount = 0;
        Integer newVersionCount = 0;
        Integer unchangedCount = 0;
        Integer linksBeforeTotal = 0;
        Integer linksAfterTotal = 0;
        for (Result r : results) {
            if (r.action == CREATED) {
                createdCount++;
            } else if (r.action == NEW_VERSION) {
                newVersionCount++;
            } else {
                unchangedCount++;
            }
            linksBeforeTotal += r.caseLinksBefore;
            linksAfterTotal += r.caseLinksAfter;
        }
        return 'RUNBOOK_SEED files=' + results.size()
            + ' created=' + createdCount
            + ' new_version=' + newVersionCount
            + ' unchanged=' + unchangedCount
            + ' case_links_before=' + linksBeforeTotal
            + ' case_links_after=' + linksAfterTotal;
    }

    /**
     * Case links per File. A ContentDocumentLink query must filter on an Id and
     * can't filter on the linked record's type, so Cases are picked out here.
     */
    private static Map<Id, Integer> caseLinkCounts(List<Id> docIds) {
        Map<Id, Integer> counts = new Map<Id, Integer>();
        Set<Id> ids = new Set<Id>();
        for (Id docId : docIds) {
            if (docId != null) {   // null: a runbook with no File yet
                ids.add(docId);
            }
        }
        if (ids.isEmpty()) {
            return counts;
        }
        for (ContentDocumentLink cdl : [
            SELECT ContentDocumentId, LinkedEntityId FROM ContentDocumentLink
            WHERE ContentDocumentId IN :ids
        ]) {
            if (cdl.LinkedEntityId.getSObjectType() != Case.SObjectType) {
                continue;
            }
            Integer n = counts.get(cdl.ContentDocumentId);
            counts.put(cdl.ContentDocumentId, n == null ? 1 : n + 1);
        }
        return counts;
    }
}
