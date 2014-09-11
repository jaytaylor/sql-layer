/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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
            throw new WrongStateException(CursorState.IDLE.toString(), cursor);
        }
    }

    public static void checkIdleOrActive(CursorBase cursor)
    {
        if (cursor.isClosed()) {
            String state = CursorState.IDLE.toString() + " OR " + CursorState.ACTIVE.toString();
            throw new WrongStateException(state, cursor);
        }
    }

    public static void checkClosed (CursorBase cursor)
    {
        if (!cursor.isClosed()) {
            throw new WrongStateException (CursorState.CLOSED.toString(), cursor);
        }
    }
    
    private static String cursorState(CursorBase cursor)
    {
        return cursor.isIdle() ? CursorState.IDLE.toString() : 
            cursor.isActive() ? CursorState.ACTIVE.toString() : CursorState.CLOSED.toString();
    }

    enum CursorState {
        CLOSED,
        IDLE,
        ACTIVE
    }

    public static class WrongStateException extends RuntimeException
    {
        WrongStateException(String expectedState, CursorBase cursor)
        {
            super(String.format("Cursor should be %s but is actually %s", expectedState, cursorState(cursor)));
        }
    }
}
