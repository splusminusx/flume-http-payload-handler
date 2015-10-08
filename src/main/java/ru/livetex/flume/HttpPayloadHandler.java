package ru.livetex.flume;

import java.io.BufferedReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import javax.servlet.http.HttpServletRequest;


/**
 * Обработчик тела HTTP запроса.
 */
public class HttpPayloadHandler implements HTTPSourceHandler {

    private static final String SERVICE_KEY = "service";
    private static final String METHOD_KEY = "method";
    private static final String UNKNOWN = "unknown";

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
        String charset = request.getCharacterEncoding();
        if (charset == null) {
            charset = "UTF-8";
        } else if (!(charset.equalsIgnoreCase("utf-8"))) {
            throw new UnsupportedCharsetException("HTTP payload handler supports UTF-8 only");
        }
        BufferedReader reader = request.getReader();
        int contentLength = request.getContentLength();
        List<Event> eventList = new ArrayList<Event>(0);

        if (contentLength > 0) {
            char[] buffer = new char[contentLength];
            Map<String, String> headers = extractHeaders(request);
            reader.read(buffer);
            eventList.add(EventBuilder.withBody(new String(buffer).getBytes(charset), headers));
        }

        return eventList;
    }

    /**
     * Извлечение из запроса хедеров для события.
     *
     * @param request - запрос.
     * @return - хедеры для события.
     */
    private Map<String, String> extractHeaders(HttpServletRequest request) {
        Map<String, String> headers = new HashMap<String, String>();

        String service = request.getParameter(SERVICE_KEY);
        if (service == null) {
            service = UNKNOWN;
        }
        headers.put(SERVICE_KEY, service);

        String method = request.getParameter(METHOD_KEY);
        if (method == null) {
            method = UNKNOWN;
        }
        headers.put(METHOD_KEY, method);
        return headers;
    }
}
