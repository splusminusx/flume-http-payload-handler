package ru.livetex.flume;

import org.apache.flume.Event;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class HttpPayloadHandlerTest {
    private static byte[] message = "[1,\"exportLeads\",1,0,{\"1\":{\"str\":\"accountId\"},\"2\":{\"str\":\"queryString\"},\"3\":{\"rec\":{\"1\":{\"i32\":1},\"2\":{\"str\":\",\"},\"3\":{\"rec\":{\"1\":{\"tf\":1}}}}}}]".getBytes();

    @Test
    public void testMessage() throws Exception {
        HttpPayloadHandler handler = new HttpPayloadHandler();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setContent(message);
        List<Event> events = handler.getEvents(request);

        assertEquals(events.size(), 1);
        Event event = events.get(0);
        assertArrayEquals(event.getBody(), message);
    }
}