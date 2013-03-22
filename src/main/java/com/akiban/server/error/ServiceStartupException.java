
package com.akiban.server.error;

public final class ServiceStartupException extends InvalidOperationException {
    public ServiceStartupException (String serviceName) {
        super (ErrorCode.SERVICE_ALREADY_STARTED, serviceName);
    }
}
