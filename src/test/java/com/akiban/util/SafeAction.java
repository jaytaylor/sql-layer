
package com.akiban.util;

public final class SafeAction {
    public interface Get<T> {
        T get();
    }

    public static <T> T of(Get<T> getMethod) {
        try {
            return getMethod.get();
        } catch (Throwable e) {
            throw new SafeActionException(e);
        }
    }

    private static class SafeActionException extends RuntimeException {
        private SafeActionException(Throwable cause) {
            super(cause);
        }
    }
}
