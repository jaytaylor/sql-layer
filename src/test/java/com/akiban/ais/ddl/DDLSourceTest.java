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

package com.akiban.ais.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.akiban.ais.model.AkibanInformationSchema;
import org.junit.Test;

import com.akiban.ais.io.CSVTarget;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;

public class DDLSourceTest {

    private final static String DDL_FILE_NAME = "src/test/resources/DDLSourceTest_schema.ddl";
    private final static String XXXXXXXX_DDL_FILE_NAME = "src/test/resources/xxxxxxxx_schema.ddl";
    private final static String XXXXXXXX_FK_DDL_FILE_NAME = "src/test/resources/xxxxxxxx_schema.ddl";

    private final static String SCHEMA_NAME = "DDLSourceTest_schema";

    private final static String[] EXPECTED_CHARSET_VALUES = new String[] { "latin1", "utf8", "utf8", "latin1", "utf8",
        "utf8" };

    private final static String[] EXPECTED_COLLATION_VALUES = new String[]{
        "utf8_general_ci", "utf8_general_ci", "utf8_general_ci",
        "latin1_german2_ci", "utf8_general_ci",
        "utf8_general_ci"};

    @Test
    public void testFKParse() throws Exception {
        final AkibanInformationSchema ais1 = new DDLSource()
                .buildAISFromFile(XXXXXXXX_DDL_FILE_NAME);
        final AkibanInformationSchema ais2 = new DDLSource()
                .buildAISFromFile(XXXXXXXX_FK_DDL_FILE_NAME);
        final StringWriter aisw1 = new StringWriter();
        final StringWriter aisw2 = new StringWriter();
        new Writer(new CSVTarget(new PrintWriter(aisw1))).save(ais1);
        new Writer(new CSVTarget(new PrintWriter(aisw2))).save(ais2);
        assertEquals(aisw1.getBuffer().toString(), aisw2.getBuffer().toString());
    }

    @Test
    public void testOverloadedTableColumn() throws Exception {
        String ddl = "CREATE TABLE `s1`.one (idOne int, PRIMARY KEY (idOne)) engine=akibandb;\n"
                + "CREATE TABLE `s2`.one (idTwo int, PRIMARY KEY (idTwo)) engine=akibandb;";

        AkibanInformationSchema ais = new DDLSource().buildAISFromString(ddl);
        assertEquals("user tables", 2, ais.getUserTables().size());
        assertEquals("group tables", 2, ais.getGroupTables().size());
        UserTable s1 = ais.getUserTable("s1", "one");
        UserTable s2 = ais.getUserTable("s2", "one");
        assertNotNull("s1", s1);
        assertNotNull("s2", s2);
        assertSame("s1 group's root", s1, ais.getGroup("one").getGroupTable()
                .getRoot());
        assertSame("s2 group's root", s2, ais.getGroup("one$0").getGroupTable()
                .getRoot());
    }

    private static void createTableWithError(
            Class<? extends Throwable> errClass, String errMessage,
            String innerDDL) {
        Throwable thrown = null;
        try {
            createTableFromInner(innerDDL);
        } catch (Throwable t) {
            thrown = t;
        }
        assertNotNull("expected " + errClass, thrown);
        assertEquals("exception class", errClass, thrown.getClass());
        assertEquals("exception message", errMessage, thrown.getMessage());
    }

    private static SchemaDef.UserTableDef createTableFromInner(String ddl)
            throws Exception {
        SchemaDef.UserTableDef ret = new DDLSource().parseCreateTable("s.t ("
                + ddl + ") engine=akibandb;");
        assertEquals("schema", "s", ret.getCName().getSchema());
        assertEquals("table", "t", ret.getCName().getName());
        assertEquals("engine", "akibandb", ret.engine);
        return ret;
    }

    /**
     * Generates a FK statement
     * 
     * @param parent
     *            the parent table name
     * @param includeConstraint
     *            whether to include the "CONSTRAINT [symbol]" part of the
     *            string
     * @param constraintName
     *            (optional) if non-null and includesConstraint is true, the
     *            constraint symbol
     * @param indexName
     *            (optional) the index name
     * @param columns
     *            the columns. Any column that starts with "parent:" will apply
     *            only to the parent; any that starts with "child:" will apply
     *            only to the child. All others will apply to both
     * @return the FK
     */
    private static String fk(String parent, boolean includeConstraint,
            String constraintName, String indexName, String... columns) {
        List<String> parentCols = new ArrayList<String>(columns.length);
        List<String> childCols = new ArrayList<String>(columns.length);
        for (String column : columns) {
            if (column.startsWith("parent")) {
                parentCols.add(column.substring("parent:".length()));
            } else if (column.startsWith("child:")) {
                childCols.add(column.substring("child:".length()));
            } else {
                parentCols.add(column);
                childCols.add(column);
            }
        }
        assertEquals("column mismatch between parent " + parentCols
                + " and child " + childCols, parentCols.size(),
                childCols.size());

        StringBuilder builder = new StringBuilder();
        if (includeConstraint) {
            builder.append("CONSTRAINT ");
            if (constraintName != null) {
                TableName.escape(constraintName, builder);
                builder.append(' ');
            }
        }
        builder.append("FOREIGN KEY ");
        if (indexName != null) {
            TableName.escape(indexName, builder);
            builder.append(' ');
        }
        builder.append('(');
        for (String column : childCols) {
            TableName.escape(column, builder);
            builder.append(',');
        }
        builder.setLength(builder.length() - 1);
        builder.append(')');

        builder.append(" REFERENCES ");
        TableName.escape(parent, builder);
        builder.append(' ');

        builder.append('(');
        for (String column : parentCols) {
            TableName.escape(column, builder);
            builder.append(',');
        }
        builder.setLength(builder.length() - 1);
        builder.append(')');

        return builder.toString();
    }
}
