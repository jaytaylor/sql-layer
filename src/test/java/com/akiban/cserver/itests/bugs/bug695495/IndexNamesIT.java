package com.akiban.cserver.itests.bugs.bug695495;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.*;

public final class IndexNamesIT extends ApiTestBase {
    private static final String BASE_DDL = "CREATE TABLE t1( c1 tinyint(4) not null, c2 int(11) DEFAULT NULL, ";

    @Test
    public void oneCustomNamedIndex() throws InvalidOperationException {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY myNamedKey(c2)");
        assertIndexes(userTable, "PRIMARY", "myNamedKey");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "myNamedKey", "c2");
    }

    @Test
    public void oneStandardNamedIndex() throws InvalidOperationException {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (`c1`), KEY `c2` (`c2`)");
        assertIndexes(userTable, "PRIMARY", "c2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
    }

    @Test
    public void oneUnnamedIndex() throws InvalidOperationException {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY (c2)");

        assertIndexes(userTable, "PRIMARY", "c2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
    }

    @Test
    public void twoIndexesNoConflict() throws InvalidOperationException {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY (c2), KEY multiColumnIndex(c2, c1)");

        assertIndexes(userTable, "PRIMARY", "c2", "multiColumnIndex");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
        assertIndexColumns(userTable, "multiColumnIndex", "c2", "c1");
    }

    @Test
    public void twoUnnamedIndexes() throws InvalidOperationException {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY (c2), KEY (c2, c1)");

        assertIndexes(userTable, "PRIMARY", "c2", "c2_2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
        assertIndexColumns(userTable, "c2_2", "c2", "c1");
    }

    protected UserTable createTableWithIndexes(String indexDDL) throws InvalidOperationException {
        ddl().createTable(session, "s1", BASE_DDL + indexDDL + ");");
        return ddl().getAIS(session).getUserTable("s1", "t1");
    }

    protected static void assertIndexes(UserTable table, String... expectedIndexNames) {
        Set<String> expectedIndexesSet = new TreeSet<String>(Arrays.asList(expectedIndexNames));
        Set<String> actualIndexes = new TreeSet<String>();
        for (Index index : table.getIndexes()) {
            String indexName = index.getIndexName().getName();
            boolean added = actualIndexes.add(indexName);
            assertTrue("duplicate index name: " + indexName, added);
        }
        assertEquals("indexes in " + table.getName(), expectedIndexesSet, actualIndexes);
    }

    protected static void assertIndexColumns(UserTable table, String indexName, String... expectedColumns) {
        Index index = table.getIndex(indexName);
        assertNotNull(indexName + " was null", index);
        List<String> actualColumns = new ArrayList<String>();
        for (IndexColumn indexColumn : index.getColumns()) {
            actualColumns.add(indexColumn.getColumn().getName());
        }
        assertEquals(indexName + " columns", actualColumns, Arrays.asList(expectedColumns));
    }
}
