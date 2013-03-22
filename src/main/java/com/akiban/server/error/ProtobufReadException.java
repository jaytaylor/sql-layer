
package com.akiban.server.error;

public class ProtobufReadException extends InvalidOperationException {
    public ProtobufReadException(String protoMessageName, String errorDetail) {
        super(ErrorCode.PROTOBUF_READ, protoMessageName, errorDetail);
    }
}
