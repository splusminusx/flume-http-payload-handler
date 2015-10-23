package ru.livetex.flume;


import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public class JsonThriftMethodInterceptor extends ThriftMethodInterceptor {
    private static final TProtocolFactory protocolFactory = new TJSONProtocol.Factory();

    @Override
    protected TProtocolFactory getTProtocolFactory() {
        return protocolFactory;
    }
}