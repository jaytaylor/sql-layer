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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.server.test.mt.mthapi.base.sais.SaisBuilder;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class BasicWriterTest {
    @Test
    public void simpleTableDDL() {
        SaisTable one = new SaisBuilder().table("zebra", "id", "stripes").pk("id").backToBuilder().getSoleRootTable();
        String ddl = DDLUtils.buildDDL(one, new StringBuilder());
        assertEquals("ddl",
                "CREATE TABLE zebra(id int not null,stripes int, PRIMARY KEY (id))",
                ddl);
    }

    @Test
    public void multiColumnDDL() {
        SaisTable one = new SaisBuilder()
                .table("zebra", "id", "stripes", "height").pk("id", "stripes") // unique per zebra!
                .backToBuilder().getSoleRootTable();
        String ddl = DDLUtils.buildDDL(one, new StringBuilder());
        assertEquals("ddl",
                "CREATE TABLE zebra(id int not null,stripes int not null,height int, PRIMARY KEY (id,stripes))",
                ddl);
    }

    @Test
    public void withJoin() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("top", "id1", "id2").pk("id1", "id2");
        builder.table("second", "tid1", "tid2").joinTo("top").col("id1", "tid1").col("id2", "tid2");
        SaisTable second = builder.getSoleRootTable().getChild("second");

        String ddl = DDLUtils.buildDDL(second, new StringBuilder());
        assertEquals("ddl",
                "CREATE TABLE second(tid1 int,tid2 int, "+
                "GROUPING FOREIGN KEY(tid1,tid2) REFERENCES top(id1,id2))",
                ddl);
    }
}
