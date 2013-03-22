
package com.akiban.server.types;

public final class WrongValueGetException extends ValueSourceException {
    public WrongValueGetException(AkType expectedType, AkType actualType) {
        super("expected to put or get " + expectedType + " but saw " + actualType);
    }
}
