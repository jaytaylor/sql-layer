/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.operator;

public final class CursorLifecycle
{
    public static void checkIdle(CursorBase cursor)
    {
        if (!cursor.isIdle()) {
            throw new WrongStateException(IDLE, cursor);
        }
    }

    public static void checkActive(CursorBase cursor)
    {
        if (!cursor.isActive()) {
            throw new WrongStateException(ACTIVE, cursor);
        }
    }

    public static void checkIdleOrActive(CursorBase cursor)
    {
        if (cursor.isDestroyed()) {
            throw new WrongStateException(IDLE_OR_ACTIVE, cursor);
        }
    }

    public static void checkDestroyed(CursorBase cursor)
    {
        if (!cursor.isDestroyed()) {
            throw new WrongStateException(DESTROYED, cursor);
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
