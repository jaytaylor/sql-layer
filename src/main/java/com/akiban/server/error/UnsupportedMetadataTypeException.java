
package com.akiban.server.error;

public class UnsupportedMetadataTypeException extends InvalidOperationException {
    public UnsupportedMetadataTypeException(String typeName) {
        super(ErrorCode.UNSUPPORTED_METADATA_TYPE, typeName);
    }
}
