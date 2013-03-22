
package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.server.error.UnsupportedIndexSizeException;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.TableDefinition;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.GrowableByteBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
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

    @Test
    public void tryRootPrimaryKeyTooLarge() throws Exception {
        createInitialSchema();
        checkInitialSchema();
        try {
            createTable("s", "t1",
                        "id varchar(2050) not null",
                        "primary key(id)");
            Assert.fail("Expected table to be rejected");
        } catch (UnsupportedIndexSizeException e) {
            // expected
        }
        checkInitialSchema();
    }

    @Test
    public void tryRootSecondaryKeyTooLarge() throws Exception {
        createInitialSchema();
        checkInitialSchema();
        try {
            createTable("s", "t1",
                        "id int not null",
                        "c1 varchar(2050)",
                        "unique(c1)",
                        "primary key(id)");
            Assert.fail("Expected table to be rejected");
        } catch (UnsupportedIndexSizeException e) {
            // expected
        }
        checkInitialSchema();
    }

    @Test
    public void tryChildPrimaryKeyTooLarge() throws Exception {
        createInitialSchema();
        checkInitialSchema();
        try {
            createTable("s", "child2",
                        "id varchar(2052) not null",
                        "pid int",
                        "primary key(id)",
                        "grouping foreign key(pid) references parent(pid)");
            Assert.fail("Expected table to be rejected");
        } catch (UnsupportedIndexSizeException e) {
            // expected
        }
        checkInitialSchema();
    }

    @Test
    public void tryChildSecondaryKeyTooLarge() throws Exception {
        createInitialSchema();
        checkInitialSchema();
        try {
            createTable("s", "child2",
                        "id int not null",
                        "pid int",
                        "filler varchar(2052)",
                        "primary key(id)",
                        "unique(filler)",
                        "grouping foreign key(pid) references parent(pid)");
            Assert.fail("Expected table to be rejected");
        } catch (UnsupportedIndexSizeException e) {
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
        createGroupingFKIndex("s", "child", "__akiban_cp", "pid");
        expectedAIS = serialize(ais());
    }

    private void checkInitialSchema() throws Exception
    {
        checkInitialAIS();
        checkInitialDDL();
    }

    private void checkInitialAIS() throws Exception
    {
        GrowableByteBuffer ais = serialize(ais());
        assertEquals(expectedAIS, ais);
    }

    private void checkInitialDDL() throws Exception
    {
        for (Map.Entry<String, TableDefinition> entry : createTableStatements("s").entrySet()) {
            String table = entry.getKey();
            String ddl = entry.getValue().getDDL();
            if (table.equals("parent")) {
                assertEquals(PARENT_DDL, ddl);
            } else if (table.equals("child")) {
                assertEquals(CHILD_DDL, ddl);
            } else {
                assertTrue(String.format("%s: %s", table, ddl), false);
            }
        }
    }

    private GrowableByteBuffer serialize(AkibanInformationSchema ais) throws Exception
    {
        GrowableByteBuffer buffer = new GrowableByteBuffer(BUFFER_SIZE);
        new ProtobufWriter(buffer).save(ais);
        buffer.flip();
        return buffer;
    }

    private Map<String, TableDefinition> createTableStatements(final String schema) throws Exception
    {
        return transactionallyUnchecked(new Callable<Map<String, TableDefinition>>() {
            @Override
            public Map<String, TableDefinition> call() throws Exception {
                return serviceManager().getServiceByClass(SchemaManager.class).getTableDefinitions(session(), schema);
            }
        });
    }

    private static final int BUFFER_SIZE = 100000; // 100K
    private static final String PARENT_DDL =
        "create table `s`.`parent`(`pid` int NOT NULL, `filler` int NULL, PRIMARY KEY(`pid`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin";
    private static final String CHILD_DDL =
        "create table `s`.`child`(`cid` int NOT NULL, `pid` int NULL, PRIMARY KEY(`cid`), "+
            "CONSTRAINT `__akiban_cp` FOREIGN KEY `__akiban_cp`(`pid`) REFERENCES `parent`(`pid`)) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin";
    private GrowableByteBuffer expectedAIS;
}
