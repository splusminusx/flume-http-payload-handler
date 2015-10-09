package ru.livetex.flume;

import org.apache.flume.Event;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class HttpPayloadHandlerTest {
    private static byte[] okMessage = "[1,\"exportLeads\",1,0,{\"1\":{\"str\":\"accountId\"},\"2\":{\"str\":\"queryString\"},\"3\":{\"rec\":{\"1\":{\"i32\":1},\"2\":{\"str\":\",\"},\"3\":{\"rec\":{\"1\":{\"tf\":1}}}}}}]".getBytes();
    private static byte[] badMessage = "BAD MESSAGE".getBytes();

    @Test
    public void testOkMessage() throws Exception {
        HttpPayloadHandler handler = new JsonHttpPayloadHandler();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setContent(okMessage);
        List<Event> events = handler.getEvents(request);

        assertEquals(events.size(), 1);
        Event event = events.get(0);

        assertEquals(event.getHeaders().get("method"), "exportLeads");
        assertArrayEquals(event.getBody(), okMessage);
    }

    @Test
    public void testBadMessage() throws Exception {
        HttpPayloadHandler handler = new JsonHttpPayloadHandler();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setContent(badMessage);
        List<Event> events = handler.getEvents(request);

        assertEquals(events.size(), 1);
        Event event = events.get(0);

        assertEquals(event.getHeaders().get("method"), "unknown");
        assertArrayEquals(event.getBody(), badMessage);
    }

}