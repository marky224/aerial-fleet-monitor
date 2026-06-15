/**
 * AFM_BriefSynthesis_Test — unit tests for the daily-brief GenAI + Chatter action.
 *
 * Both outbound seams (Models API + Chatter) are faked, so these run with no
 * real callout. The real ConnectApiPoster / ModelsApiClient seams are not
 * exercised here (same convention as AFM_TriageNarrative's ModelsApiClient).
 */
@isTest
private class AFM_BriefSynthesis_Test {

    private class FakeGenAi implements AFM_TriageNarrative.AFM_GenAiClient {
        public String captured;
        private String reply;
        public FakeGenAi(String reply) { this.reply = reply; }
        public String complete(String prompt) { this.captured = prompt; return reply; }
    }

    private class FakePoster implements AFM_BriefSynthesis.ChatterPoster {
        public String groupId;
        public String text;
        public String post(String groupId, String text) {
            this.groupId = groupId;
            this.text = text;
            return '0D5FAKEFEED0001';
        }
    }

    private static void putRequest(Map<String, Object> body) {
        RestRequest req = new RestRequest();
        req.requestUri = '/services/apexrest/afm/brief-synthesis';
        req.httpMethod = 'POST';
        req.requestBody = Blob.valueOf(JSON.serialize(body));
        RestContext.request = req;
        RestContext.response = new RestResponse();
    }

    @isTest
    static void generatesAndPosts() {
        FakeGenAi fakeAi = new FakeGenAi('Daily ops summary\n- 3 cases');
        FakePoster fakePoster = new FakePoster();
        AFM_BriefSynthesis.genai = fakeAi;
        AFM_BriefSynthesis.poster = fakePoster;
        putRequest(new Map<String, Object>{
            'prompt' => 'Summarize the day',
            'chatterGroupId' => '0F9000000000001',
            'subject' => 'Daily Brief — West'
        });

        Test.startTest();
        AFM_BriefSynthesis.BriefResult res = AFM_BriefSynthesis.generate();
        Test.stopTest();

        System.assertEquals('Daily ops summary\n- 3 cases', res.generatedText, 'returns model text');
        System.assertEquals('0D5FAKEFEED0001', res.feedElementId, 'returns feed id');
        System.assertEquals('0F9000000000001', fakePoster.groupId, 'posts to the group');
        System.assert(fakePoster.text.startsWith('Daily Brief — West'), 'subject prepended');
        System.assert(fakePoster.text.contains('Daily ops summary'), 'body included');
        System.assertEquals('Summarize the day', fakeAi.captured, 'prompt passed to model');
    }

    @isTest
    static void skipsChatterWhenNoGroup() {
        AFM_BriefSynthesis.genai = new FakeGenAi('text only');
        FakePoster fakePoster = new FakePoster();
        AFM_BriefSynthesis.poster = fakePoster;
        putRequest(new Map<String, Object>{ 'prompt' => 'x' });

        Test.startTest();
        AFM_BriefSynthesis.BriefResult res = AFM_BriefSynthesis.generate();
        Test.stopTest();

        System.assertEquals('text only', res.generatedText);
        System.assertEquals(null, res.feedElementId, 'no chatter post without a group');
        System.assertEquals(null, fakePoster.groupId, 'poster not invoked');
    }

    @isTest
    static void requiresPrompt() {
        AFM_BriefSynthesis.genai = new FakeGenAi('unused');
        AFM_BriefSynthesis.poster = new FakePoster();
        putRequest(new Map<String, Object>{ 'prompt' => '' });

        Boolean threw = false;
        Test.startTest();
        try {
            AFM_BriefSynthesis.generate();
        } catch (AFM_BriefSynthesis.AFM_BriefException e) {
            threw = true;
        }
        Test.stopTest();
        System.assert(threw, 'a blank prompt must throw AFM_BriefException');
    }
}
