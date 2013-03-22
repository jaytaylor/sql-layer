
package com.akiban.server.error;

import java.util.List;

public class CircularDependencyException extends InvalidOperationException {
    public CircularDependencyException(String forClassName, List<String> classNames ) {
        super (ErrorCode.SERVICE_CIRC_DEPEND, forClassName, classNames);
    }
}
