
package com.akiban.server.error;

public final class ServiceNotStartedException extends InvalidOperationException {
    public ServiceNotStartedException (String serviceName) {
        super (ErrorCode.SERVICE_NOT_STARTED, serviceName);
    }
}    
