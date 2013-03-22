
package com.akiban.server.error;

public class UnsupportedMetadataVersionException extends InvalidOperationException {
    public UnsupportedMetadataVersionException(int currentVersion, int unsupportedVersion) {
        super(ErrorCode.UNSUPPORTED_METADATA_VERSION, currentVersion, unsupportedVersion);
    }
}
