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
