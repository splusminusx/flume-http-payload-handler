package ru.livetex.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TimestampPrependerInterceptor implements Interceptor {
    private static final String TIMESTAMP = "timestamp";
    private static final short LONG_CAPACITY = 8;

    public void initialize() { }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        long ts = System.currentTimeMillis();
        try {
            String tsString = headers.get(TIMESTAMP);
            ts = Long.parseLong(tsString);
        } catch (NumberFormatException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
        ByteArrayOutputStream os = new ByteArrayOutputStream(LONG_CAPACITY + event.getBody().length);
        DataOutputStream dos = new DataOutputStream(os);
        try {
            dos.writeLong(ts);
            dos.write(event.getBody());
            return EventBuilder.withBody(os.toByteArray(), headers);
        } catch (Exception ignored) {
            ignored.printStackTrace();
            return null;
        }
    }

    public List<Event> intercept(List<Event> events) {
        ArrayList<Event> interceptedEvents = new ArrayList<Event>(events.size());
        for (Event event : events) {
            interceptedEvents.add(intercept(event));
        }
        return interceptedEvents;
    }

    public void close() { }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new TimestampPrependerInterceptor();
        }

        public void configure(Context context) { }
    }
}
