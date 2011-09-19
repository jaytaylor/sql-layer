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

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.rule.RulesTestHelper;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;
import static junit.framework.Assert.*;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;

import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

@RunWith(Parameterized.class)
public class OperatorCompilerTest_New extends TestBase implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "operator-new");
    
    protected File schemaFile, indexFile;
    protected SQLParser parser;
    protected OperatorCompiler_New compiler;

    @Before
    public void makeCompiler() throws Exception {
        parser = new SQLParser();
        AkibanInformationSchema ais = OptimizerTestBase.parseSchema(schemaFile);
        if (indexFile != null)
            OptimizerTestBase.loadGroupIndexes(ais, indexFile);
        compiler = TestOperatorCompiler.create(parser, ais, "user");
    }

    static class TestResultColumn extends PhysicalResultColumn {
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
    
    public static class TestOperatorCompiler extends OperatorCompiler_New {
        public static OperatorCompiler_New create(SQLParser parser, 
                                              AkibanInformationSchema ais, 
                                              String defaultSchemaName) {
            RulesTestHelper.ensureRowDefs(ais);
            return new TestOperatorCompiler(parser, ais, "user");
        }

        private TestOperatorCompiler(SQLParser parser, 
                                     AkibanInformationSchema ais, 
                                     String defaultSchemaName) {
            super(parser, ais, defaultSchemaName);
        }

        @Override
        public PhysicalResultColumn getResultColumn(String name, DataTypeDescriptor sqlType,
                                                    boolean nameDefaulted, Column column) {
            String type = String.valueOf(sqlType);
            if (column != null) {
                if (nameDefaulted)
                    // Prefer the case stored in AIS to parser's standardized form.
                    name = column.getName();
                type = column.getTypeDescription() +
                    "[" + column.getType().encoding() + "]";
            }
            return new TestResultColumn(name, type);
        }
    }

    @Parameters
    public static Collection<Object[]> statements() throws Exception {
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
        return result;
    }

    public OperatorCompilerTest_New(String caseName, File schemaFile, File indexFile,
                                String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.schemaFile = schemaFile;
        this.indexFile = indexFile;
    }

    @Test
    public void testOperator() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        BasePlannable result = compiler.compile((DMLStatementNode)stmt, 
                                                parser.getParameterList());
        return result.toString();
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEqualsWithoutHashes(caseName, expected, result);
    }

}
