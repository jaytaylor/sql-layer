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
import com.akiban.sql.NamedParamsTestBase;

import com.akiban.sql.optimizer.simplified.SimplifiedQuery;

import com.akiban.server.rowdata.RowDef;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.pg.PostgresSessionTracer;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import static junit.framework.Assert.*;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.TableStatus;
import com.akiban.server.rowdata.RowDef;

import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

@RunWith(NamedParameterizedRunner.class)
public class OperatorCompiler_OldTest extends NamedParamsTestBase
                                  implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "operator-old");
    
    protected File schemaFile, indexFile;
    protected SQLParser parser;
    protected OperatorCompiler_Old compiler;

    @Before
    public void makeCompiler() throws Exception {
        parser = new SQLParser();
        AkibanInformationSchema ais = OptimizerTestBase.parseSchema(schemaFile);
        if (indexFile != null)
            OptimizerTestBase.loadGroupIndexes(ais, indexFile);
        compiler = TestOperatorCompiler.create(parser, ais, "user");
    }

    static class TestResultColumn extends OperatorCompiler_Old.ResultColumnBase {
        private String type;

        public TestResultColumn(String name, String type) {
            super(name);
            this.type = type;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return getName() + ":" + getType();
        }
    }
    
    public static class TestOperatorCompiler extends OperatorCompiler_Old {
        public static OperatorCompiler_Old create(SQLParser parser, 
                                              AkibanInformationSchema ais, 
                                              String defaultSchemaName) {
            // This just needs to be enough to keep from UserTableRowType
            // constructor from getting NPE.
            for (UserTable userTable : ais.getUserTables().values()) {
                int tableId = userTable.getTableId();
                TableStatus ts = new TableStatus(tableId);
                ts.setOrdinal(tableId);
                new RowDef(userTable, ts);
            }
            return new TestOperatorCompiler(parser, ais, "user");
        }

        private TestOperatorCompiler(SQLParser parser, 
                                     AkibanInformationSchema ais, 
                                     String defaultSchemaName) {
            super(parser, ais, defaultSchemaName);
        }

        @Override
        public ResultColumnBase getResultColumn(SimplifiedQuery.SimpleSelectColumn selectColumn) {
            String name = selectColumn.getName();
            String type = String.valueOf(selectColumn.getType());
            if (selectColumn.getExpression().isColumn()) {
                Column column = ((SimplifiedQuery.ColumnExpression)
                                 selectColumn.getExpression()).getColumn();
                if (selectColumn.isNameDefaulted())
                    // Prefer the case stored in AIS to parser's standardized form.
                    name = column.getName();
                type = column.getTypeDescription() + 
                    "[" + column.getType().encoding() + "]";
            }
            return new TestResultColumn(name, type);
        }
    }

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<Object[]>();
        for (File subdir : RESOURCE_DIR.listFiles(new FileFilter() {
                public boolean accept(File file) {
                    return file.isDirectory();
                }
            })) {
            File schemaFile = new File(subdir, "schema.ddl");
            if (schemaFile.exists()) {
                File indexFile = new File(subdir, "group.idx");
                if (!indexFile.exists())
                    indexFile = null;
                for (Object[] args : sqlAndExpected(subdir)) {
                    Object[] nargs = new Object[args.length+2];
                    nargs[0] = subdir.getName() + "/" + args[0];
                    nargs[1] = schemaFile;
                    nargs[2] = indexFile;
                    System.arraycopy(args, 1, nargs, 3, args.length-1);
                    result.add(nargs);
                }
            }
        }
        return namedCases(result);
    }

    public OperatorCompiler_OldTest(String caseName, File schemaFile, File indexFile,
                                String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.schemaFile = schemaFile;
        this.indexFile = indexFile;
    }

    @Test
    @Ignore
    public void testOperator() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        OperatorCompiler_Old.Result result = 
            compiler.compile(new PostgresSessionTracer(1, false),
                             (DMLStatementNode)stmt,
                             parser.getParameterList());
        return result.toString();
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEqualsWithoutHashes(caseName, expected, result);
    }

}
