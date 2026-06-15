/**
 * AFM_BriefSynthesis — Phase-11 daily-brief GenAI + Chatter action.
 *
 * Synchronous Apex REST endpoint called by the AFM API (client-credentials,
 * run-as admin). Given a fully-composed prompt and a Chatter group id it:
 *   1. generates the brief text via the Salesforce Models API
 *      (aiplatform.ModelsAPI — the same proven path as AFM_TriageNarrative), then
 *   2. posts the result to that group's Chatter feed via ConnectApi.
 * Returns {generatedText, feedElementId}.
 *
 * Both outbound calls sit behind seams (genai / poster) so the Apex test injects
 * fakes — no real callout, no ConnectApi.setTest plumbing.
 *
 * No Anthropic; no external api.salesforce.com gateway (not provisioned on this
 * org — see the Phase-11 build notes). Reuses AFM_TriageNarrative.ModelsApiClient.
 */
@RestResource(urlMapping='/afm/brief-synthesis/*')
global with sharing class AFM_BriefSynthesis {

    @TestVisible
    private static AFM_TriageNarrative.AFM_GenAiClient genai =
        new AFM_TriageNarrative.ModelsApiClient();

    @TestVisible
    private static ChatterPoster poster = new ConnectApiPoster();

    // Chatter text segments are capped at 10,000 characters.
    private static final Integer CHATTER_SEGMENT_MAX = 10000;

    global class BriefRequest {
        global String prompt;
        global String chatterGroupId;
        global String subject;
    }

    global class BriefResult {
        global String generatedText;
        global String feedElementId;
    }

    @HttpPost
    global static BriefResult generate() {
        RestRequest req = RestContext.request;
        BriefRequest input = (req != null && req.requestBody != null)
            ? (BriefRequest) JSON.deserialize(req.requestBody.toString(), BriefRequest.class)
            : null;

        if (input == null || String.isBlank(input.prompt)) {
            throw new AFM_BriefException('prompt is required');
        }

        BriefResult result = new BriefResult();
        result.generatedText = genai.complete(input.prompt);

        if (String.isNotBlank(input.chatterGroupId) && String.isNotBlank(result.generatedText)) {
            String body = String.isNotBlank(input.subject)
                ? input.subject + '\n\n' + result.generatedText
                : result.generatedText;
            result.feedElementId = poster.post(input.chatterGroupId, body.abbreviate(CHATTER_SEGMENT_MAX));
        }
        return result;
    }

    public class AFM_BriefException extends Exception {}

    /** Seam for the Chatter post (the only branch not exercised by unit tests). */
    public interface ChatterPoster {
        String post(String groupId, String text);
    }

    public class ConnectApiPoster implements ChatterPoster {
        public String post(String groupId, String text) {
            ConnectApi.FeedItemInput item = new ConnectApi.FeedItemInput();
            ConnectApi.MessageBodyInput msgBody = new ConnectApi.MessageBodyInput();
            ConnectApi.TextSegmentInput seg = new ConnectApi.TextSegmentInput();
            seg.text = text;
            msgBody.messageSegments = new List<ConnectApi.MessageSegmentInput>{ seg };
            item.body = msgBody;
            item.subjectId = groupId;
            item.feedElementType = ConnectApi.FeedElementType.FeedItem;
            ConnectApi.FeedElement fe = ConnectApi.ChatterFeeds.postFeedElement(null, item);
            return fe.id;
        }
    }
}
