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

import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.functions.FunctionsRegistryImpl;
import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.TestBase;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.SQLParser;

import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.rule.IndexEstimator;
import com.akiban.sql.optimizer.rule.RulesTestHelper;
import com.akiban.sql.optimizer.rule.TestIndexEstimator;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;

import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

@RunWith(NamedParameterizedRunner.class)
public class OperatorCompilerTest extends NamedParamsTestBase 
                                  implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "operator");

    protected File schemaFile, indexFile, statsFile;
    protected SQLParser parser;
    protected OperatorCompiler compiler;

    @Before
    public void makeCompiler() throws Exception {
        parser = new SQLParser();
        AkibanInformationSchema ais = OptimizerTestBase.parseSchema(schemaFile);
        if (indexFile != null)
            OptimizerTestBase.loadGroupIndexes(ais, indexFile);
        compiler = TestOperatorCompiler.create(parser, ais, 
                                               OptimizerTestBase.DEFAULT_SCHEMA,
                                               new FunctionsRegistryImpl(),
                                               new TestIndexEstimator(ais, OptimizerTestBase.DEFAULT_SCHEMA, statsFile));
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
    
    public static class TestOperatorCompiler extends OperatorCompiler {
        public static OperatorCompiler create(SQLParser parser, 
                                              AkibanInformationSchema ais, 
                                              String defaultSchemaName,
                                              FunctionsRegistry functionsRegistry,
                                              IndexEstimator indexEstimator) {
            RulesTestHelper.ensureRowDefs(ais);
            return new TestOperatorCompiler(parser, ais, defaultSchemaName, functionsRegistry, indexEstimator);
        }

        private TestOperatorCompiler(SQLParser parser, 
                                     AkibanInformationSchema ais, 
                                     String defaultSchemaName,
                                     FunctionsRegistry functionsRegistry,
                                     IndexEstimator indexEstimator) {
            super(parser, ais, defaultSchemaName, functionsRegistry, indexEstimator);
        }

        @Override
        public PhysicalResultColumn getResultColumn(ResultField field) {
            String type = String.valueOf(field.getSQLtype());
            Column column = field.getAIScolumn();
            if (column != null) {
                type = column.getTypeDescription() +
                    "[" + column.getType().encoding() + "]";
            }
            return new TestResultColumn(field.getName(), type);
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
                File statsFile = new File(subdir, "stats.yaml");
                if (!statsFile.exists())
                    statsFile = null;
                for (Object[] args : sqlAndExpected(subdir)) {
                    Object[] nargs = new Object[args.length+3];
                    nargs[0] = subdir.getName() + "/" + args[0];
                    nargs[1] = schemaFile;
                    nargs[2] = indexFile;
                    nargs[3] = statsFile;
                    System.arraycopy(args, 1, nargs, 4, args.length-1);
                    result.add(nargs);
                }
            }
        }
        return namedCases(result);
    }

    public OperatorCompilerTest(String caseName, 
                                File schemaFile, File indexFile, File statsFile,
                                String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.schemaFile = schemaFile;
        this.indexFile = indexFile;
        this.statsFile = statsFile;
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
    public void checkResult(String result) throws IOException{
        assertEqualsWithoutHashes(caseName, expected, result);
    }

}
