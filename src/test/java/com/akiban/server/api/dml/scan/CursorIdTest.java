
package com.akiban.server.api.dml.scan;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

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
