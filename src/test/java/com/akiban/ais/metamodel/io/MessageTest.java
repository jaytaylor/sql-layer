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

package com.akiban.ais.metamodel.io;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.rowdata.SchemaFactory;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public final class MessageTest {
    private static AkibanInformationSchema coiSchema(boolean withGroupIndex) throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory("test");
        return schemaFactory.ais("create table test.c(id int not null primary key, name varchar(32));",
                                 "create table test.o(id int not null primary key, cid int, foo int, ",
                                 "grouping foreign key(cid) references c(id));",
                                 "create table test.i(id int not null primary key, oid int, ",
                                 "grouping foreign key(oid) references o(id));",
                                 withGroupIndex ? "create index foo on o(c.name, o.foo) using left join;" : "");
    }

    private static void serializeAndCompare(AkibanInformationSchema ais) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1 << 19);
        new Writer(new MessageTarget(buffer)).save(ais);
        buffer.flip();
        AkibanInformationSchema newAis = new Reader(new MessageSource(buffer)).load();
        assertEquals("buffer left", 0, buffer.remaining());
        assertEquals("groups", ais.getGroups().keySet(), newAis.getGroups().keySet());
        assertEquals("user tables", ais.getUserTables().keySet(), newAis.getUserTables().keySet());
        assertEquals("group tables", ais.getGroupTables().keySet(), newAis.getGroupTables().keySet());
        assertEquals("joins", ais.getJoins().keySet(), newAis.getJoins().keySet());
        newAis.checkIntegrity();
    }

    @Test
    public void emptyAIS() throws Exception {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        serializeAndCompare(ais);
    }

    @Test
    public void coiAIS() throws Exception {
        AkibanInformationSchema ais = coiSchema(false);
        serializeAndCompare(ais);
    }

    @Test
    public void coiAISWithGroupIndex() throws Exception {
        final AkibanInformationSchema ais = coiSchema(true);
        final Table cTable = ais.getTable("test", "c");
        assertNotNull("c table not null", cTable);
        final Table oTable = ais.getTable("test", "o");
        assertNotNull("o table not null", oTable);
        final Group group = cTable.getGroup();
        assertSame("customer and order group", group, oTable.getGroup());
        ais.checkIntegrity();
        serializeAndCompare(ais);
    }

    @Test
    public void unicodeSchemaAndTableNames() throws Exception {
        NewAISBuilder builder = AISBBasedBuilder.create();
        AkibanInformationSchema ais =
            builder
            .userTable("test", "☃").colLong("id").pk("id")
            .userTable("☂", "rain").colLong("id").pk("id")
            .userTable("☾", "☽").colLong("id").pk("id")
            .ais();
        serializeAndCompare(ais);
    }
}
