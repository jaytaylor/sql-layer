
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.NamedParamsTestBase;

import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.rule.ASTStatementLoader;
import com.akiban.sql.optimizer.rule.BaseRule;
import com.akiban.sql.optimizer.rule.PlanContext;
import com.akiban.sql.optimizer.rule.RulesTestContext;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;

import com.akiban.ais.model.AkibanInformationSchema;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static junit.framework.Assert.*;

import java.io.File;
import java.util.*;

@RunWith(NamedParameterizedRunner.class)
public class DuplicatePlanTest extends OptimizerTestBase
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "plan/duplicate");

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<>();
        for (File sqlFile : listSQLFiles(RESOURCE_DIR)) {
            String caseName = sqlFile.getName().replace(".sql", "");
            String sql = fileContents(sqlFile);
            result.add(new Object[] {
                           caseName, sql
                       });
        }
        return NamedParamsTestBase.namedCases(result);
    }

    private AkibanInformationSchema ais;

    public DuplicatePlanTest(String caseName, String sql) {
        super(caseName, sql, null, null);
    }

    @Before
    public void loadDDL() throws Exception {
        ais = loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
    }

    @Test
    public void testDuplicatePlan() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        // Turn parsed AST into intermediate form.
        PlanNode plan = new AST((DMLStatementNode)stmt, parser.getParameterList());
        {
            RulesTestContext rulesContext = 
                RulesTestContext.create(ais, null, false,
                                        Collections.<BaseRule>singletonList(new ASTStatementLoader()), new Properties());
            PlanContext planContext = new PlanContext(rulesContext, plan);
            rulesContext.applyRules(planContext);
            plan = planContext.getPlan();
        }
        PlanNode duplicate = (PlanNode)plan.duplicate();
        assertFalse(plan == duplicate);
        assertEqualsWithoutHashes(caseName, 
                                  PlanToString.of(plan), 
                                  PlanToString.of(duplicate));
    }

}
