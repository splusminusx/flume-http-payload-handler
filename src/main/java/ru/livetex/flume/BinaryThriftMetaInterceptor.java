package ru.livetex.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.livetex.kafka.gen.meta.MetaHolder;
import ru.livetex.kafka.gen.meta.MetaHolder$;

import java.util.List;


public class BinaryThriftMetaInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(BinaryThriftMetaInterceptor.class);
    private static final String MESSAGE_TYPE_KEY = "message_type";
    private static final String UNKNOWN_NAME = "unknown";
    private static final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory(true, true);

    public void initialize() { }

    public Event intercept(Event event) {
        String messageType = extractMessageType(event.getBody());
        event.getHeaders().put(MESSAGE_TYPE_KEY, messageType);
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() { }

    private String extractMessageType(byte[] bytes) {
        TTransport transport = new TMemoryInputTransport(bytes);
        TProtocol protocol = protocolFactory.getProtocol(transport);
        try {
            MetaHolder holder = MetaHolder$.MODULE$.decode(protocol);
            return Integer.toString(holder.meta().messageType());
        } catch (Exception e) {
            logger.warn("Can't extract livetex-meta from event");
            return UNKNOWN_NAME;
        }
    }

    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new BinaryThriftMetaInterceptor();
        }

        public void configure(Context context) { }
    }
}
