package ru.livetex.flume;


import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public class JsonThriftMethodInterceptor extends ThriftMethodInterceptor {
    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new JsonThriftMethodInterceptor();
        }

        public void configure(Context context) {
        }
    }

    private static final TProtocolFactory protocolFactory = new TJSONProtocol.Factory();

    @Override
    protected TProtocolFactory getTProtocolFactory() {
        return protocolFactory;
    }
}
