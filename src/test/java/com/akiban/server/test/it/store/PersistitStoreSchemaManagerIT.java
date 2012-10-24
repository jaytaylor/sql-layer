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

package com.akiban.server.test.it.store;

import org.junit.Test;

import java.util.Set;

import static com.akiban.server.test.it.store.SchemaManagerIT.*;
import static com.akiban.server.store.PersistitStoreSchemaManager.SerializationType;
import static org.junit.Assert.assertEquals;

public class PersistitStoreSchemaManagerIT extends PersistitStoreSchemaManagerITBase {
    @Test
    public void newDataSetReadAndSavedAsProtobuf() throws Exception {
        createTable(SCHEMA, T1_NAME, T1_DDL);
        assertEquals("Saved as PROTOBUF", SerializationType.PROTOBUF, pssm.getSerializationType());

        safeRestart();

        assertEquals("Saw PROTOBUF on load", SerializationType.PROTOBUF, pssm.getSerializationType());
    }

    @Test
    public void delayedTreeRemoval() throws Exception {
        int tid = createTable(SCHEMA, T1_NAME, T1_DDL);
        for(int i = 0; i < 5; ++i) {
            writeRow(tid, i);
        }

        String groupTreeName = getUserTable(tid).getGroup().getTreeName();
        String pkTreeName = getUserTable(tid).getPrimaryKey().getIndex().getTreeName();

        Set<String> treeNames = pssm.getTreeNames();
        assertEquals("Group tree is in set before drop", true, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set before drop", true, treeNames.contains(pkTreeName));

        ddl().dropTable(session(), tableName(SCHEMA, T1_NAME));

        treeNames = pssm.getTreeNames();
        assertEquals("Group tree is in set after drop", true, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set after drop", true, treeNames.contains(pkTreeName));

        safeRestart();

        treeNames = pssm.getTreeNames();
        assertEquals("Group tree is in set after restart", false, treeNames.contains(groupTreeName));
        assertEquals("PK tree is in set after restart", false, treeNames.contains(pkTreeName));

        assertEquals("Group tree exist after restart", false, treeService().treeExists(SCHEMA, groupTreeName));
        assertEquals("PK tree exists after restart", false, treeService().treeExists(SCHEMA, pkTreeName));
    }
}
