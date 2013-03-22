
package com.akiban.qp.rowtype;

public final class FieldHasNoColumnException extends RuntimeException {
    public FieldHasNoColumnException(int i) {
        super("at index " + i);
    }
}
