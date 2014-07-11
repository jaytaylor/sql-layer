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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.DDLGenerator;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import static junit.framework.Assert.*;

public class AtomicSchemaChangesIT extends ITBase
{
    // The tests catch Throwable, because some of the breakage scenarios actually survive the DDL and AIS
    // layers, and instead cause createTable to fail an assertion.

    @Test
    public void tryBadSyntax() throws Exception
    {
        createInitialSchema();
        checkInitialSchema();
        try {
            createTable("s", "bad_syntax",
                        "foo bar");
            fail();
        } catch (RuntimeException e) {
            // expected
        }
        checkInitialSchema();
    }

    @Test
    public void tryFailValidation() throws Exception
    {
        createInitialSchema();
        checkInitialSchema();
        try {
            createTable("s", "fail_validation",
                        "bid int not null primary key",
                        "pid int",
                        "grouping foreign key (pid) references parent(no_such_column)");
            fail();
        } catch (Throwable e) {
            // expected
        }
        checkInitialSchema();
    }

    @Test
    public void tryFailAISCreation_1() throws Exception
    {
        createInitialSchema();
        checkInitialSchema();
        try {
            createTable("s", "fail_ais_creation_1",
                        "bid int not null primary key",
                        "pid int",
                        "unique(xyz)");
            fail();
        } catch (Throwable e) {
            // expected
        }
        checkInitialSchema();
    }

    private void createInitialSchema() throws Exception
    {
        createTable("s", "parent",
                    "pid int not null primary key",
                    "filler int");
        createTable("s", "child",
                    "cid int not null primary key",
                    "pid int",
                    "grouping foreign key (pid) references parent(pid)");
        createGroupingFKIndex("s", "child", "s/parent/pid/s/s.child/pid", "pid");
        expectedAIS = serialize(ais());
    }

    private void checkInitialSchema() throws Exception
    {
        checkInitialAIS();
        checkInitialDDL();
    }

    private void checkInitialAIS() throws Exception
    {
        ByteBuffer ais = serialize(ais());
        assertEquals(expectedAIS, ais);
    }

    private void checkInitialDDL() throws Exception
    {
        for (Map.Entry<String, String> entry : createTableStatements("s").entrySet()) {
            String table = entry.getKey();
            String ddl = entry.getValue();
            if (table.equals("parent")) {
                assertEquals(PARENT_DDL, ddl);
            } else if (table.equals("child")) {
                assertEquals(CHILD_DDL, ddl);
            } else {
                assertTrue(String.format("%s: %s", table, ddl), false);
            }
        }
    }

    private ByteBuffer serialize(AkibanInformationSchema ais) throws Exception
    {
        ProtobufWriter writer = new ProtobufWriter();
        writer.save(ais);
        ByteBuffer buffer = ByteBuffer.allocate(writer.getBufferSize());
        writer.serialize(buffer);
        buffer.flip();
        return buffer;
    }

    private Map<String, String> createTableStatements(final String schema) throws Exception
    {
        return transactionallyUnchecked(new Callable<Map<String, String>>() {
            @Override
            public Map<String, String> call() throws Exception {
                DDLGenerator generator = new DDLGenerator();
                Map<String, String> map = new TreeMap<>();
                for(Table table : ais().getSchema(schema).getTables().values()) {
                    map.put(table.getName().getTableName(), generator.createTable(table));
                }
                return map;
            }
        });
    }

    private static final String PARENT_DDL =
        "create table `s`.`parent`(`pid` int NOT NULL, `filler` int NULL, PRIMARY KEY(`pid`)) engine=akibandb DEFAULT CHARSET=UTF8 COLLATE=UCS_BINARY";
    private static final String CHILD_DDL =
        "create table `s`.`child`(`cid` int NOT NULL, `pid` int NULL, PRIMARY KEY(`cid`), "+
            "CONSTRAINT `__akiban_cp` FOREIGN KEY `__akiban_cp`(`pid`) REFERENCES `parent`(`pid`)) engine=akibandb DEFAULT CHARSET=UTF8 COLLATE=UCS_BINARY";
    private ByteBuffer expectedAIS;
}
