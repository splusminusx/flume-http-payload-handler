package ru.livetex.flume;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.regex.Pattern;


public abstract class ThriftMethodInterceptor implements Interceptor {
    private static final String METHOD_KEY = "method";
    /**
     * Имя метода, когда невозможно распарсить thrift сообщение
     */
    private static final String UNKNOWN_NAME = "unknown";
    /**
     * Имя метода, когда thrift сообщение было распаршено, но имя содержит невалидные символы
     */
    private static final String INVALID_NAME = "invalid";
    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9]+");

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
            String methodName = protocol.readMessageBegin().name;

            if (isValidMethodName(methodName)) {
                return methodName;
            } else {
                return INVALID_NAME;
            }
        } catch (TException e) {
            e.printStackTrace();
            return UNKNOWN_NAME;
        }
    }

    /**
     * Проверить валидность имени метода. Допустимы только символы латиницей и цифры
     * @param methodName Имя метода для очистки
     * @return Очищенное от невалидных символов имя метода
     */
    private boolean isValidMethodName(String methodName) {
        return VALID_NAME_PATTERN.matcher(methodName).matches();
    }
}
