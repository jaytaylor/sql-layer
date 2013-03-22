
package com.akiban.server.error;

public class MissingDDLParametersException extends InvalidOperationException {
    public MissingDDLParametersException () {
        super (ErrorCode.MISSING_DDL_PARAMETERS);
    }
}
