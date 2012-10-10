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

package com.akiban.server.test.it.bugs.bug701580;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public final class SpuriousDuplicateKeyIT extends ITBase {
    @Test
    public void simpleOnce() throws Exception {
        simpleTestCase();
    }

    @Test
    public void simpleTwice() throws Exception {
        simpleTestCase();
        simpleTestCase();
    }

    private void simpleTestCase() throws Exception {
        createTable("test", "t1", "bid1 int not null, token varchar(64), primary key(bid1)");
        createIndex("test", "t1", "token", "token");
        int t2 = createTable("test", "t2", "bid int not null, theme varchar(64), primary key (bid), unique(theme)");

        confirmIds("t1", 1, 2);
        confirmIds("t2", 1, 2);

        writeRows(
                createNewRow(t2, 1, "0"),
                createNewRow(t2, 2, "1"),
                createNewRow(t2, 3, "2")
        );
        dropAllTables();
    }

    @Test
    public void indexIdsLocalToGroup() throws Exception {
        createTable("test", "t1", "bid1 int not null, token varchar(64), primary key(bid1)");
        createIndex("test", "t1", "token", "token");

        createTable("test", "t2", "bid int not null, theme varchar(64), primary key (bid), unique (theme)");
        createTable("test", "t3", "id int not null primary key, bid_id int, "+
                    "GROUPING FOREIGN KEY (bid_id) REFERENCES t2 (bid)");
        createIndex("test", "t3", "__akiban_fk", "bid_id");

        confirmIds("t1", 1, 2);
        confirmIds("t2", 1, 2);
        confirmIds("t3", 3, 2);
    }

    /**
     * Confirm that the given table has sequential index IDs starting from the given number, and that its
     * group table has all those indexes as well.
     * @param tableName the table to start at
     * @param startingAt the index to start at
     * @param expectedUIndexes how many indexes you expect on the user table
     * @throws Exception if there's a problem!
     */
    private void confirmIds(String tableName, int startingAt, int expectedUIndexes)
            throws Exception {
        UserTable uTable = ddl().getAIS(session()).getUserTable("test", tableName);

        Set<Integer> expectedUTableIds = new HashSet<Integer>();
        Set<Integer> actualUTableIds = new HashSet<Integer>();
        for (Index index : uTable.getIndexes()) {
            actualUTableIds.add(index.getIndexId());
            expectedUTableIds.add( expectedUTableIds.size() + startingAt );
        }

        assertEquals("uTable index count", expectedUIndexes, actualUTableIds.size());
    }
}
