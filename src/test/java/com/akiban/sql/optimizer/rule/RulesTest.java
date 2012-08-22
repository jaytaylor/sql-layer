/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.TestBase;

import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.plan.AST;
import com.akiban.sql.optimizer.plan.PlanToString;
import com.akiban.sql.optimizer.rule.PlanContext;

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

    protected File rulesFile, schemaFile, indexFile, statsFile, propertiesFile, extraDDL;

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
                File defaultStatsFile = new File(subdir, "stats.yaml");
                File defaultPropertiesFile = new File(subdir, "compiler.properties");
                File defaultExtraDDL = new File(subdir, "schema-extra.ddl");
                if (!defaultStatsFile.exists())
                    defaultStatsFile = null;
                if (!defaultPropertiesFile.exists())
                    defaultPropertiesFile = null;
                if (!defaultExtraDDL.exists())
                    defaultExtraDDL = null;
                for (Object[] args : sqlAndExpected(subdir)) {
                    File statsFile = new File(subdir, args[0] + ".stats.yaml");
                    File propertiesFile = new File(subdir, args[0] + ".properties");
                    File extraDDL = new File(subdir, args[0] + ".ddl");
                    if (!statsFile.exists())
                        statsFile = defaultStatsFile;
                    if (!propertiesFile.exists())
                        propertiesFile = defaultPropertiesFile;
                    if (!extraDDL.exists())
                        extraDDL = defaultExtraDDL;
                    Object[] nargs = new Object[args.length+5];
                    nargs[0] = subdir.getName() + "/" + args[0];
                    nargs[1] = rulesFile;
                    nargs[2] = schemaFile;
                    nargs[3] = statsFile;
                    nargs[4] = propertiesFile;
                    nargs[5] = extraDDL;
                    System.arraycopy(args, 1, nargs, 6, args.length-1);
                    result.add(nargs);
                }
            }
        }
        return NamedParamsTestBase.namedCases(result);
    }

    public RulesTest(String caseName, 
                     File rulesFile, File schemaFile, File statsFile, File propertiesFile,
                     File extraDDL,
                     String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.rulesFile = rulesFile;
        this.schemaFile = schemaFile;
        this.statsFile = statsFile;
        this.propertiesFile = propertiesFile;
        this.extraDDL = extraDDL;
    }

    protected RulesContext rules;

    @Before
    public void loadDDL() throws Exception {
        List<File> schemaFiles = new ArrayList<File>(2);
        schemaFiles.add(schemaFile);
        if (extraDDL != null)
            schemaFiles.add(extraDDL);
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
        rules = RulesTestContext.create(ais, statsFile, extraDDL != null,
                                        RulesTestHelper.loadRules(rulesFile), 
                                        properties);
        // Normally set as a consequence of OutputFormat.
        binder.setAllowSubqueryMultipleColumns(Boolean.parseBoolean(properties.getProperty("allowSubqueryMultipleColumns",
                                                                                           "false")));
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
