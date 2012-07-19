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

package com.akiban.server.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.error.NoSuchGroupException;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GroupIndexCreatorTest {
    private AkibanInformationSchema ais;

    @Before
    public void setup() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        ais = builder.defaultSchema("test")
                .userTable("c").colLong("id").colString("name", 32).pk("id")
                .userTable("o").colLong("id").colLong("date").colLong("cid").pk("id").joinTo("c").on("cid", "id")
                .unvalidatedAIS();
    }

    @Test(expected=NoSuchGroupException.class)
    public void unknownGroup() throws Exception {
        GroupIndexCreator.createIndex(ais, "foobar", "name_date", "c.name", Index.JoinType.LEFT);
    }

    @Test
    public void singleTableSingleColumnLeft() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(ais, "c", "c_name", "c.name", Index.JoinType.LEFT);
        final Group cGroup = ais.getGroup("c");
        final UserTable cTable = ais.getUserTable("test", "c");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("index name", "c_name", index.getIndexName().getName());
        assertEquals("column count", 1, index.getKeyColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getKeyColumns().get(0).getColumn());
        assertEquals("join type", Index.JoinType.LEFT, index.getJoinType());
        assertTrue("join not valid", index.isValid());
    }

    @Test
    public void singleTableSingleColumnRight() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(ais, "c", "c_name", "c.name", Index.JoinType.RIGHT);
        final Group cGroup = ais.getGroup("c");
        final UserTable cTable = ais.getUserTable("test", "c");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("index name", "c_name", index.getIndexName().getName());
        assertEquals("column count", 1, index.getKeyColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getKeyColumns().get(0).getColumn());
        assertEquals("join type", Index.JoinType.RIGHT, index.getJoinType());
        assertTrue("join not valid", index.isValid());
    }

    @Test
    public void twoTablesTwoColumns() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(ais, "c", "name_date", "c.name,o.date", Index.JoinType.LEFT);
        final Group cGroup = ais.getGroup("c");
        final UserTable cTable = ais.getUserTable("test", "c");
        final UserTable oTable = ais.getUserTable("test", "o");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("right name", "name_date", index.getIndexName().getName());
        assertEquals("column count", 2, index.getKeyColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getKeyColumns().get(0).getColumn());
        assertEquals("col2 is o.date", oTable.getColumn("date"), index.getKeyColumns().get(1).getColumn());
        assertEquals("join type", Index.JoinType.LEFT, index.getJoinType());
        assertTrue("join not valid", index.isValid());
    }
}
