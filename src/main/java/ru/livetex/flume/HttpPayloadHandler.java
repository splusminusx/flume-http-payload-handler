package ru.livetex.flume;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;


/**
 * Обработчик тела HTTP запроса.
 */
public class HttpPayloadHandler implements HTTPSourceHandler {

    private static final String METHOD_KEY = "method";
    private static final String UNKNOWN = "unknown";
    private static final TProtocolFactory protocolFactory = new TJSONProtocol.Factory();

    @Override
    public void configure(Context context) {
    }

    /**
     * Порождение событий из HTTP запроса.
     *
     * @param request - запрос.
     * @return - список событий.
     * @throws Exception
     */
    public List<Event> getEvents(HttpServletRequest request) throws Exception {
        List<Event> eventList = new ArrayList<Event>(0);
        int contentLength = request.getContentLength();
        if (contentLength > 0) {
            byte[] payload = extractPayload(request);
            Map<String, String> headers = extractHeaders(payload);
            eventList.add(EventBuilder.withBody(payload, headers));
        }

        return eventList;
    }

    private byte[] extractPayload(HttpServletRequest request) throws IOException {
        ServletInputStream inputStream = request.getInputStream();
        byte[] buffer = new byte[request.getContentLength()];
        inputStream.read(buffer);
        return buffer;
    }

    /**
     * Извлечение из запроса хедеров для события.
     *
     * @param payload - запрос.
     * @return - хедеры для события.
     */
    private Map<String, String> extractHeaders(byte[] payload) {
        Map<String, String> headers = new HashMap<String, String>();

        String methodName = UNKNOWN;
        try {
            methodName = extractMethodName(payload);
        } catch (TException e) {
            e.printStackTrace();
        }
        headers.put(METHOD_KEY, methodName);
        return headers;
    }

    /**
     * Извлечение названия thrift-метода из сообщения.
     * @param payload - запрос.
     * @return - название thrift-метода.
     * @throws TException
     */
    private String extractMethodName(byte[] payload) throws TException {
        TTransport transport = new TMemoryInputTransport(payload);
        TProtocol protocol = protocolFactory.getProtocol(transport);
        return protocol.readMessageBegin().name;
    }
}
