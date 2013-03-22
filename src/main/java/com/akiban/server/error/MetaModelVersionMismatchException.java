
package com.akiban.server.error;

public class MetaModelVersionMismatchException extends InvalidOperationException {
    public MetaModelVersionMismatchException () {
        super (ErrorCode.METAMODEL_MISMATCH);
    }
}
