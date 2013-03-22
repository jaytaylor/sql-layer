
package com.akiban.qp.operator;

public final class CursorLifecycle
{
    public static void checkIdle(CursorBase cursor)
    {
        if (!cursor.isIdle()) {
            throw new WrongStateException(IDLE, cursor);
        }
    }

    public static void checkIdleOrActive(CursorBase cursor)
    {
        if (cursor.isDestroyed()) {
            throw new WrongStateException(IDLE_OR_ACTIVE, cursor);
        }
    }

    private static String cursorState(CursorBase cursor)
    {
        return cursor.isIdle() ? IDLE : cursor.isActive() ? ACTIVE : DESTROYED;
    }

    private static String IDLE = "IDLE";
    private static String ACTIVE = "ACTIVE";
    private static String DESTROYED = "DESTROYED";
    private static String IDLE_OR_ACTIVE = IDLE + " or " + ACTIVE;

    public static class WrongStateException extends RuntimeException
    {
        WrongStateException(String expectedState, CursorBase cursor)
        {
            super(String.format("Cursor should be %s but is actually %s", expectedState, cursorState(cursor)));
        }
    }
}
