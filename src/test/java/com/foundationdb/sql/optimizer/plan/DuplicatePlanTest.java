/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.sql.NamedParamsTestBase;

import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.RulesTestContext;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.ais.model.AkibanInformationSchema;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

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
