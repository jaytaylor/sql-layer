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
    public void oneCustomNamedIndex() {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY myNamedKey(c2)");
        assertIndexes(userTable, "PRIMARY", "myNamedKey");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "myNamedKey", "c2");
    }

    @Test
    public void oneStandardNamedIndex() {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (`c1`), KEY `c2` (`c2`)");
        assertIndexes(userTable, "PRIMARY", "c2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
    }

    @Test
    public void oneUnnamedIndex() {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY (c2)");

        assertIndexes(userTable, "PRIMARY", "c2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
    }

    @Test
    public void twoIndexesNoConflict() {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY (c2), KEY multiColumnIndex(c2, c1)");

        assertIndexes(userTable, "PRIMARY", "c2", "multiColumnIndex");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
        assertIndexColumns(userTable, "multiColumnIndex", "c2", "c1");
    }

    @Test
    public void twoUnnamedIndexes() {
        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY (c2), KEY (c2, c1)");

        assertIndexes(userTable, "PRIMARY", "c2", "c2_2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
        assertIndexColumns(userTable, "c2_2", "c2", "c1");
    }

    @Test
    public void fkFullyNamed() {
        UserTable userTable = createTableWithFK("fk_constraint_1", "fk_index_1", null);
        assertIndexes(userTable, "PRIMARY", "fk_constraint_1");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "fk_constraint_1", "c2");
    }

    @Test
    public void fkOnlyConstraintNamed() {
        UserTable userTable = createTableWithFK("fk_constraint_1", null, null);
        assertIndexes(userTable, "PRIMARY", "fk_constraint_1");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "fk_constraint_1", "c2");
    }

    @Test
    public void fkOnlyIndexNamed() {
        UserTable userTable = createTableWithFK(null, "fk_index_1", null);
        assertIndexes(userTable, "PRIMARY", "fk_index_1");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "fk_index_1", "c2");
    }

    @Test
    public void fkNeitherNamed() {
        UserTable userTable = createTableWithFK(null, null, null);
        assertIndexes(userTable, "PRIMARY", "c2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
    }

    @Test
    public void fkWithSingleColumnKeyUnnamed() {
        UserTable userTable = createTableWithFK(null, null, "key (c2)");
        assertIndexes(userTable, "PRIMARY", "c2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2");
    }

    @Test
    public void fkWithSingleColumnKeyNamed() {
        UserTable userTable = createTableWithFK(null, null, "key `index_2` (c2)");
        assertIndexes(userTable, "PRIMARY", "index_2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "index_2", "c2");
    }

    @Test
    public void fkWithTwoColumnKeyUnnamed() {
        UserTable userTable = createTableWithFK(null, null, "key (c2, c1)");
        assertIndexes(userTable, "PRIMARY", "c2");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "c2", "c2", "c1");
    }

    @Test
    public void fkWithTwoColumnKeyNamed() {
        UserTable userTable = createTableWithFK(null, null, "key `index_twocol` (c2, c1)");
        assertIndexes(userTable, "PRIMARY", "index_twocol");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "index_twocol", "c2", "c1");
    }

    @Test
    public void fkUsingExplicitKey() {
        UserTable userTable = createTableWithFK("my_constraint", "my_key", "key my_constraint(c2)");
        assertIndexes(userTable, "PRIMARY", "my_constraint");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "my_constraint", "c2");
    }

    @Test
    public void fkUsingExplicitKeyConflicting() throws InvalidOperationException {
        createTableWithFK("my_constraint", "my_key", "key my_constraint(c1)");
        fail("should have failed at the above line!");
    }

    protected static void debug(String formatter, Object... args) {
        if(Boolean.getBoolean("IndexNamesIT.debug")) {
            System.out.print("\tIndexNamesIT: ");
            System.out.println(String.format(formatter, args));
        }
    }

    protected UserTable createTableWithIndexes(String indexDDL) {
        final String ddl = BASE_DDL + indexDDL + ");";
        debug(ddl);
        try {
            ddl().createTable(session, "s1", ddl);
        } catch (InvalidOperationException e) {
            throw new RuntimeException("creating DDL: " + ddl, e);
        }
        return ddl().getAIS(session).getUserTable("s1", "t1");
    }

    protected UserTable createTableWithFK(String constraintName, String indexName, String additionalIndexes) {
        try {
            ddl().createTable(session, "s1", "CREATE TABLE p1(parentc1 int key)");
        } catch (InvalidOperationException e) {
            throw new RuntimeException(e);
        }

        StringBuilder builder = new StringBuilder("PRIMARY KEY (c1), ");
        if (additionalIndexes != null) {
            builder.append(additionalIndexes).append(", ");
        }
        builder.append("CONSTRAINT ");
        if (constraintName != null) {
            builder.append(constraintName).append(' ');
        }
        builder.append("FOREIGN KEY ");
        if (indexName != null) {
            builder.append(indexName).append(' ');
        }
        builder.append("(c2) REFERENCES p1(parentc1)");

        return createTableWithIndexes(builder.toString());
    }

    protected static void assertIndexes(UserTable table, String... expectedIndexNames) {
        Set<String> expectedIndexesSet = new TreeSet<String>(Arrays.asList(expectedIndexNames));
        debug("for table %s, expecting indexes %s", table, expectedIndexesSet);
        Set<String> actualIndexes = new TreeSet<String>();
        for (Index index : table.getIndexes()) {
            String indexName = index.getIndexName().getName();
            boolean added = actualIndexes.add(indexName);
            assertTrue("duplicate index name: " + indexName, added);
        }
        assertEquals("indexes in " + table.getName(), expectedIndexesSet, actualIndexes);
    }

    protected static void assertIndexColumns(UserTable table, String indexName, String... expectedColumns) {
        List<String> expectedColumnsList = Arrays.asList(expectedColumns);
        debug("for index %s.%s, expecting columns %s", table, indexName, expectedColumnsList);
        Index index = table.getIndex(indexName);
        assertNotNull(indexName + " was null", index);
        List<String> actualColumns = new ArrayList<String>();
        for (IndexColumn indexColumn : index.getColumns()) {
            actualColumns.add(indexColumn.getColumn().getName());
        }
        assertEquals(indexName + " columns", actualColumns, expectedColumnsList);
    }
}
