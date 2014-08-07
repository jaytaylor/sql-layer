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

package com.foundationdb.server.api.dml.scan;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CursorIdTest
{
    @Test
    public void test()
    {
        final int TABLES = 5;
        final int CURSORS = 5;
        final int SESSIONS = 2;
        for (long sessionId1 = 0; sessionId1 < SESSIONS; sessionId1++) {
            for (int tableId1 = 0; tableId1 < TABLES; tableId1++) {
                for (long cursorId1 = 0; cursorId1 < CURSORS; cursorId1++) {
                    CursorId cid1 = new CursorId(sessionId1, cursorId1, tableId1);
                    for (long sessionId2 = 0; sessionId2 < SESSIONS; sessionId2++) {
                        for (int tableId2 = 0; tableId2 < TABLES; tableId2++) {
                            for (long cursorId2 = 0; cursorId2 < CURSORS; cursorId2++) {
                                CursorId cid2 = new CursorId(sessionId2, cursorId2, tableId2);
                                assertEquals(sessionId1 == sessionId2 && tableId1 == tableId2 && cursorId1 == cursorId2,
                                             cid1.equals(cid2));
                                assertTrue(!cid1.equals(cid2) || cid1.hashCode() == cid2.hashCode());
                            }
                        }
                    }
                }
            }
        }
    }
}
