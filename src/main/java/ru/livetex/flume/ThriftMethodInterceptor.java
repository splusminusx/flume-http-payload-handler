package ru.livetex.flume;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.util.List;


public abstract class ThriftMethodInterceptor implements Interceptor {
    private static final String METHOD_KEY = "method";
    private static final String UNKNOWN = "unknown";

    protected abstract TProtocolFactory getTProtocolFactory();

    public void initialize() {

    }

    public Event intercept(Event event) {
        String methodName = extractMethodName(event.getBody());
        event.getHeaders().put(METHOD_KEY, methodName);
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {

    }

    /**
     * Извлечение названия thrift-метода из сообщения.
     *
     * @param payload - запрос.
     * @return - название thrift-метода.
     */
    private String extractMethodName(byte[] payload) {
        TTransport transport = new TMemoryInputTransport(payload);
        TProtocol protocol = getTProtocolFactory().getProtocol(transport);
        try {
            return cleanMethodName(protocol.readMessageBegin().name);
        } catch (TException e) {
            e.printStackTrace();
            return UNKNOWN;
        }
    }

    /**
     * Очистить имя метода от невалидных символово
     * Допустимы только символы латиницей и цифры
     * @param methodName Имя метода для очистки
     * @return Очищенное от невалидных символов имя метода
     */
    private String cleanMethodName(String methodName) {
        return methodName.replaceAll("[^a-zA-Z0-9]", "");
    }
}
