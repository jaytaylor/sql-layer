
package com.akiban.server.types;

public final class ValueSourceIsNullException extends ValueSourceException {
    public ValueSourceIsNullException() {
    }

    static void checkNotNull(ValueSource valueSource) {
        if (valueSource.isNull()) {
            throw new ValueSourceIsNullException();
        }
    }
}
