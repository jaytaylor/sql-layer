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

import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.TestBase;

import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.plan.AST;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.PlanToString;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;

import com.akiban.ais.model.AkibanInformationSchema;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;

@RunWith(NamedParameterizedRunner.class)
public class RulesTest extends OptimizerTestBase 
                       implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "rule");

    protected File rulesFile, schemaFile, indexFile, statsFile, propertiesFile, defaultExtraDDL, extraDDL;

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<Object[]>();
        for (File subdir : RESOURCE_DIR.listFiles(new FileFilter() {
                public boolean accept(File file) {
                    return file.isDirectory();
                }
            })) {
            File rulesFile = new File(subdir, "rules.yml");
            File schemaFile = new File(subdir, "schema.ddl");
            if (rulesFile.exists() && schemaFile.exists()) {
                File statsFile = new File(subdir, "stats.yaml");
                if (!statsFile.exists())
                    statsFile = null;
                File compilerPropertiesFile = new File(subdir, "compiler.properties");
                File defaultExtraDDL = new File(subdir, "schema-extra.ddl");
                if (!compilerPropertiesFile.exists())
                    compilerPropertiesFile = null;
                for (Object[] args : sqlAndExpected(subdir)) {
                    File propertiesFile = new File(subdir, args[0] + ".properties");
                    File extraDDL = new File(subdir, args[0] + ".ddl");
                    if (!propertiesFile.exists())
                        propertiesFile = compilerPropertiesFile;
                    Object[] nargs = new Object[args.length+6];
                    nargs[0] = subdir.getName() + "/" + args[0];
                    nargs[1] = rulesFile;
                    nargs[2] = schemaFile;
                    nargs[3] = statsFile;
                    nargs[4] = propertiesFile;
                    nargs[5] = defaultExtraDDL;
                    nargs[6] = extraDDL;
                    System.arraycopy(args, 1, nargs, 7, args.length-1);
                    result.add(nargs);
                }
            }
        }
        return NamedParamsTestBase.namedCases(result);
    }

    public RulesTest(String caseName, 
                     File rulesFile, File schemaFile, File statsFile, File propertiesFile,
                     File defaultExtraDDL, File extraDDL,
                     String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.rulesFile = rulesFile;
        this.schemaFile = schemaFile;
        this.statsFile = statsFile;
        this.propertiesFile = propertiesFile;
        this.defaultExtraDDL = defaultExtraDDL;
        this.extraDDL = extraDDL;
    }

    protected RulesContext rules;

    @Before
    public void loadDDL() throws Exception {
        List<File> schemaFiles = new ArrayList<File>(2);
        schemaFiles.add(schemaFile);
        if (extraDDL.exists())
            schemaFiles.add(extraDDL);
        else if (defaultExtraDDL.exists())
            schemaFiles.add(defaultExtraDDL);
        AkibanInformationSchema ais = loadSchema(schemaFiles);
        Properties properties = new Properties();
        if (propertiesFile != null) {
            FileInputStream fstr = new FileInputStream(propertiesFile);
            try {
                properties.load(fstr);
            }
            finally {
                fstr.close();
            }
        }
        rules = new RulesTestContext(ais, DEFAULT_SCHEMA, statsFile,
                                     RulesTestHelper.loadRules(rulesFile),
                                     properties);
    }

    @Test
    public void testRules() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        // Turn parsed AST into intermediate form as starting point.
        PlanContext plan = new PlanContext(rules, 
                                           new AST((DMLStatementNode)stmt,
                                                   parser.getParameterList()));
        rules.applyRules(plan);
        return PlanToString.of(plan.getPlan());
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEqualsWithoutHashes(caseName, expected, result);
    }

}
