package ru.livetex.flume;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public class BinaryThriftMethodInterceptor extends ThriftMethodInterceptor {
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new BinaryThriftMethodInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }

    private static final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory(true, true);

    @Override
    protected TProtocolFactory getTProtocolFactory() {
        return protocolFactory;
    }
}
