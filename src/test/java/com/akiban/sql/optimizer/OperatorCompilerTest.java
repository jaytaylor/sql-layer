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

package com.akiban.sql.optimizer;

import com.akiban.sql.TestBase;
import com.akiban.sql.parser.CursorNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.SQLParser;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowDef;
import com.akiban.server.TableStatus;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.test.it.qp.TestRow;

import org.junit.Before;

import java.io.File;
import java.util.Collection;

@RunWith(Parameterized.class)
public class OperatorCompilerTest extends TestBase
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "operator");
    
    protected SQLParser parser;
    protected OperatorCompiler compiler;

    @Before
    public void makeCompiler() throws Exception {
        parser = new SQLParser();
        AkibanInformationSchema ais = loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
        // This just needs to be enough to keep from UserTableRowType
        // constructor from getting NPE.
        int tableId = 0;
        for (UserTable userTable : ais.getUserTables().values()) {
            new RowDef(userTable, new TableStatus(++tableId));
        }
        compiler = new TestOperatorCompiler(parser, ais, "user");
    }

    protected static AkibanInformationSchema loadSchema(File schema) throws Exception {
        String sql = fileContents(schema);
        SchemaDef schemaDef = SchemaDef.parseSchema("use user; " + sql);
        SchemaDefToAis toAis = new SchemaDefToAis(schemaDef, false);
        return toAis.getAis();
    }

    static class TestOperatorCompiler extends OperatorCompiler {
        public TestOperatorCompiler(SQLParser parser, 
                                    AkibanInformationSchema ais, String defaultSchemaName) {
            super(parser, ais, defaultSchemaName);
        }

        @Override
        protected Row getIndexRow(Index index, Object[] keys) {
            IndexRowType rowType = schema.indexRowType(index);
            return new TestRow(rowType, keys);
        }
    }

    @Parameters
    public static Collection<Object[]> statements() throws Exception {
        return sqlAndExpected(RESOURCE_DIR);
    }

    public OperatorCompilerTest(String caseName, String sql, String expected) {
        super(caseName, sql, expected);
    }

    @Test
    public void testOperator() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        OperatorCompiler.Result result = compiler.compile((CursorNode)stmt);
        assertEqualsWithoutHashes(caseName, expected, result.toString());
    }

}
