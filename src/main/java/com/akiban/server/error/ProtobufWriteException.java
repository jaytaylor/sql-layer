
package com.akiban.server.error;

public class ProtobufWriteException extends InvalidOperationException {
    public ProtobufWriteException(String protoMessageName, String errorDetail) {
        super(ErrorCode.PROTOBUF_WRITE, protoMessageName, errorDetail);
    }
}
