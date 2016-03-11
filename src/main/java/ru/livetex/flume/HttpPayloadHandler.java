package ru.livetex.flume;

import java.io.BufferedReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;


/**
 * Обработчик тела HTTP запроса.
 */
public class HttpPayloadHandler implements HTTPSourceHandler {

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
        int contentLength = request.getContentLength();
        List<Event> eventList = new ArrayList<Event>(0);

        if (contentLength > 0) {
            ServletInputStream inputStream = request.getInputStream();
            byte[] buffer = new byte[request.getContentLength()];
            inputStream.read(buffer);
            eventList.add(EventBuilder.withBody(buffer));
        }
        return eventList;
    }
}
