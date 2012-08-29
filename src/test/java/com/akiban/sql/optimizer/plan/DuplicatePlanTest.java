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
        Collection<Object[]> result = new ArrayList<Object[]>();
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
