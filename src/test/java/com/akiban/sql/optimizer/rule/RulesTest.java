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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.TestBase;

import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.AST;
import com.akiban.sql.optimizer.plan.PlanToString;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

@RunWith(Parameterized.class)
public class RulesTest extends OptimizerTestBase implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "rule");

    protected File rulesFile, schemaFile;

    @Parameters
    public static Collection<Object[]> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<Object[]>();
        for (File subdir : RESOURCE_DIR.listFiles(new FileFilter() {
                public boolean accept(File file) {
                    return file.isDirectory();
                }
            })) {
            File rulesFile = new File(subdir, "rules.yml");
            File schemaFile = new File(subdir, "schema.ddl");
            if (rulesFile.exists() && schemaFile.exists()) {
                for (Object[] args : sqlAndExpected(subdir)) {
                    Object[] nargs = new Object[args.length+2];
                    nargs[0] = subdir.getName() + "/" + args[0];
                    nargs[1] = rulesFile;
                    nargs[2] = schemaFile;
                    System.arraycopy(args, 1, nargs, 3, args.length-1);
                    result.add(nargs);
                }
            }
        }
        return result;
    }

    public RulesTest(String caseName, File rulesFile, File schemaFile,
                     String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.rulesFile = rulesFile;
        this.schemaFile = schemaFile;
    }

    protected List<BaseRule> rules;

    @Before
    public void loadRules() throws Exception {
        rules = RulesTestHelper.loadRules(rulesFile);
    }

    @Before
    public void loadDDL() throws Exception {
        RulesTestHelper.ensureRowDefs(loadSchema(schemaFile));
    }

    @Test
    public void testRules() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        String result = null;
        Exception errorResult = null;
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        // Turn parsed AST into intermediate form as starting point.
        PlanNode plan = new AST((DMLStatementNode)stmt);
        for (BaseRule rule : rules) {
            plan = rule.apply(plan);
        }
        return PlanToString.of(plan);
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEqualsWithoutHashes(caseName, expected, result);
    }

}
