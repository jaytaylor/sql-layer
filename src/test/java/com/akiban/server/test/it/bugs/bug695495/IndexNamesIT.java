/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.bugs.bug695495;

import com.akiban.ais.model.UserTable;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.SchemaDefParseException;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import org.junit.After;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.*;

public final class IndexNamesIT extends ITBase {
    private static final String BASE_DDL = "CREATE TABLE t1(\n\tc1 tinyint(4) not null, c2 int(11) DEFAULT NULL, ";

    @After
    public void myTearDown() {
        debug("------------------------------------");
    }

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
    public void fkWithExplicitKeyAndFullName() {
        UserTable userTable = createTableWithFK("bravo", "charlie", "key alpha(c2)");
        assertIndexes(userTable, "PRIMARY", "alpha");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "alpha", "c2");
    }

    @Test
    public void uniqueWithExplicitKeyAndFullName() {
        try {
            ddl().createTable(session(), "s1", "CREATE TABLE p1(parentc1 int key)");
        } catch (InvalidOperationException e) {
            throw new TestException("CREATE TABLE p1(parentc1 int key)", e);
        }

        UserTable userTable = createTableWithIndexes("PRIMARY KEY (c1), KEY alpha (c2), "
                +"CONSTRAINT bravo UNIQUE KEY charlie (c2)");
        assertIndexes(userTable, "PRIMARY", "alpha", "charlie");
        assertIndexColumns(userTable, "PRIMARY", "c1");
        assertIndexColumns(userTable, "alpha", "c2");
        assertIndexColumns(userTable, "charlie", "c2");
    }

    @Test(expected=SchemaDefParseException.class)
    public void fkUsingExplicitKeyConflicting() throws InvalidOperationException {
        try {
            createTableWithFK("my_constraint", "my_key", "key my_constraint(c1)");
        } catch (TestException e) {
            throw e.getCause();
        }
        fail("should have failed at the above line!");
    }

    @Test
    public void fkAndKeySingleColumnSeparate() throws Exception {
        createParentTables();
        UserTable userTable = createTablePlain("c1",
                "id INT KEY",
                "c1 INT",
                "c2 INT",
                "KEY key1 (c1, c2)",
                "CONSTRAINT __akiban_fk_1 FOREIGN KEY __akiban_fk_5 (c2) REFERENCES p1 (parentc1)"
        );
        assertIndexes(userTable, "PRIMARY", "key1", "__akiban_fk_1");
        assertIndexColumns(userTable, "PRIMARY", "id");
        assertIndexColumns(userTable, "key1", "c1", "c2");
        assertIndexColumns(userTable, "__akiban_fk_1", "c2");
    }

    @Test
    public void fkAndKeySingleColumnSame() throws Exception {
        createParentTables();
        UserTable userTable = createTablePlain("c1",
                "id INT KEY",
                "c1 INT",
                "c2 INT",
                "KEY key1 (c1, c2)",
                "CONSTRAINT __akiban_fk_1 FOREIGN KEY __akiban_fk_5 (c1) REFERENCES p1 (parentc1)"
        );
        assertIndexes(userTable, "PRIMARY", "key1", "__akiban_fk_1");
        assertIndexColumns(userTable, "PRIMARY", "id");
        assertIndexColumns(userTable, "key1", "c1", "c2");
        assertIndexColumns(userTable, "__akiban_fk_1", "c1");
    }

    @Test
    public void fkAndKeyMultiColumnSame() throws Exception {
        createParentTables();
        UserTable userTable = createTablePlain("c1",
                "id INT KEY",
                "c1 INT",
                "c2 INT",
                "c3 INT",
                "KEY key1 (c1, c2, c3)",
                "CONSTRAINT __akiban_fk_1 FOREIGN KEY __akiban_fk_5 (c1,c2) REFERENCES p2 (parentc1, parentc2)"
        );
        assertIndexes(userTable, "PRIMARY", "key1", "__akiban_fk_1");
        assertIndexColumns(userTable, "PRIMARY", "id");
        assertIndexColumns(userTable, "key1", "c1", "c2", "c3");
        assertIndexColumns(userTable, "__akiban_fk_1", "c1", "c2");
    }

    @Test
    public void fkAndKeyMultiColumnDifferent() throws Exception {
        createParentTables();
        UserTable userTable = createTablePlain("c1",
                "id INT KEY",
                "c1 INT",
                "c2 INT",
                "c3 INT",
                "KEY key1 (c1, c2, c3)",
                "CONSTRAINT __akiban_fk_1 FOREIGN KEY __akiban_fk_5 (c2,c3) REFERENCES p2 (parentc1, parentc2)"
        );
        assertIndexes(userTable, "PRIMARY", "key1", "__akiban_fk_1");
        assertIndexColumns(userTable, "PRIMARY", "id");
        assertIndexColumns(userTable, "key1", "c1", "c2", "c3");
        assertIndexColumns(userTable, "__akiban_fk_1", "c2", "c3");
    }

    @Test
    public void fkAndKeyMultiColumnWrongOrder() throws Exception {
        createParentTables();
        UserTable userTable = createTablePlain("c1",
                "id INT KEY",
                "c1 INT",
                "c2 INT",
                "c3 INT",
                "KEY key1 (c1, c2, c3)",
                "CONSTRAINT __akiban_fk_1 FOREIGN KEY __akiban_fk_5 (c2,c1) REFERENCES p2 (parentc1, parentc2)"
        );
        assertIndexes(userTable, "PRIMARY", "key1", "__akiban_fk_1");
        assertIndexColumns(userTable, "PRIMARY", "id");
        assertIndexColumns(userTable, "key1", "c1", "c2", "c3");
        assertIndexColumns(userTable, "__akiban_fk_1", "c2", "c1");
    }

    protected static void debug(String formatter, Object... args) {
        if(Boolean.getBoolean("IndexNamesIT.debug")) {
            String[] lines = String.format(formatter, args).split("\n");
            for (String line : lines) {
                System.out.print("\tIndexNamesIT: ");
                System.out.println(line);
            }
        }
    }

    protected UserTable createTablePlain(String tableName, String... definition) {
        StringBuilder ddlBuilder = new StringBuilder("CREATE TABLE s1.").append(tableName).append("(\n\t");
        ddlBuilder.append(Strings.join(Arrays.asList(definition), ",\n\t"));
        ddlBuilder.append("\n)");
        String ddl = ddlBuilder.toString();
        debug(ddl);
        try {
            ddl().createTable(session(), "s1", ddl);
        } catch (InvalidOperationException e) {
            throw new TestException("creating DDL: " + ddl, e);
        }
        return ddl().getAIS(session()).getUserTable("s1", tableName);
    }

    protected UserTable createTableWithIndexes(String indexDDL) {
        String ddl = BASE_DDL + indexDDL;
        ddl = ddl.replaceAll(", *", ",\n\t");
        ddl += "\n);";
        debug(ddl);
        try {
            ddl().createTable(session(), "s1", ddl);
        } catch (InvalidOperationException e) {
            throw new TestException("creating DDL: " + ddl, e);
        }
        return ddl().getAIS(session()).getUserTable("s1", "t1");
    }

    protected void createParentTables() {
        final String p1 = "CREATE TABLE p1(parentc1 int key)";
        final String p2 = "CREATE TABLE p2(parentc1 int, parentc2 int, PRIMARY KEY (parentc1, parentc2))";
        try {
            ddl().createTable(session(), "s1", p1);
        } catch (InvalidOperationException e) {
            throw new TestException(p1, e);
        }
        try {
            ddl().createTable(session(), "s1", p2);
        } catch (InvalidOperationException e) {
            throw new TestException(p2, e);
        }
    }

    protected UserTable createTableWithFK(String constraintName, String indexName, String additionalIndexes) {
        createParentTables();
        
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

    private void assertIndexes(UserTable table, String... expectedIndexNames) {
        // TODO convenience method for ApiTestBase call. This method, and assertIndexColumns below,
        // were designed before the ApiTestBase methods. These convenience methods are here to make the transition
        // to the new ApiTestBase methods less invasive to this class.
        expectIndexes(tableId(table.getName()), expectedIndexNames);
    }

    private void assertIndexColumns(UserTable table, String indexName, String... expectedColumns) {
        expectIndexColumns(tableId(table.getName()), indexName, expectedColumns);
    }
}
