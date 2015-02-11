package ru.livetex.flume;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import javax.servlet.http.HttpServletRequest;


/**
 * Обработчик тела HTTP запроса.
 */
public class HttpPayloadHandler implements HTTPSourceHandler {

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
        BufferedReader reader = request.getReader();
        int contentLength = request.getContentLength();
        List<Event> eventList = new ArrayList<Event>(0);

        if (contentLength > 0) {
            char[] buffer = new char[contentLength];

            reader.read(buffer);
            eventList.add(EventBuilder.withBody(new String(buffer).getBytes()));
        }

        return eventList;
    }
}
