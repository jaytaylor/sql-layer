
package com.akiban.server.error;

public class UnsupportedConfigurationException extends InvalidOperationException {
    public UnsupportedConfigurationException(String variable) {
        super(ErrorCode.UNSUPPORTED_CONFIGURATION, variable);
    }
}
