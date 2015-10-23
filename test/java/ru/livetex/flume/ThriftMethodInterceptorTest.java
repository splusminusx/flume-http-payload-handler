package ru.livetex.flume;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class ThriftMethodInterceptorTest {
    private static byte[] okMessage = "[1,\"exportLeads\",1,0,{\"1\":{\"str\":\"accountId\"},\"2\":{\"str\":\"queryString\"},\"3\":{\"rec\":{\"1\":{\"i32\":1},\"2\":{\"str\":\",\"},\"3\":{\"rec\":{\"1\":{\"tf\":1}}}}}}]".getBytes();
    private static byte[] badMessage = "BAD MESSAGE".getBytes();

    @Test
    public void testOkMessage() throws Exception {
        ThriftMethodInterceptor handler = new JsonThriftMethodInterceptor();
        Event event = handler.intercept(EventBuilder.withBody(okMessage));

        assertEquals(event.getHeaders().get("method"), "exportLeads");
        assertArrayEquals(event.getBody(), okMessage);
    }

    @Test
    public void testBadMessage() throws Exception {
        ThriftMethodInterceptor handler = new JsonThriftMethodInterceptor();
        Event event = handler.intercept(EventBuilder.withBody(badMessage));

        assertEquals(event.getHeaders().get("method"), "unknown");
        assertArrayEquals(event.getBody(), badMessage);
    }

}