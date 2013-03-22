
package com.akiban.qp.operator;

public final class BindingNotSetException extends RuntimeException
{
    public BindingNotSetException(Throwable cause, int position) {
        super(message(position), cause);
    }

    public BindingNotSetException(int position) {
        super(message(position));
    }

    private static String message(int position) {
        return "binding not set at index " + position;
    }
}
