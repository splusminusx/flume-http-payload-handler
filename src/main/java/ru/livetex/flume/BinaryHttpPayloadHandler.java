package ru.livetex.flume;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public class BinaryHttpPayloadHandler extends HttpPayloadHandler {
    private static final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

    @Override
    protected TProtocolFactory getTProtocolFactory() {
        return protocolFactory;
    }
}
