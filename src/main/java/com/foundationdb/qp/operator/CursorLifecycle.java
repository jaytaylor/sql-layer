/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.operator;

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
