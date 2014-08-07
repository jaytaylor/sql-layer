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

package com.foundationdb.sql.optimizer.rule.join_enum;

import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.plan.*;
import static com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.optimizer.rule.RulesContext;
import com.foundationdb.sql.optimizer.rule.RulesTestContext;
import com.foundationdb.sql.optimizer.rule.RulesTestHelper;

import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.ais.model.AkibanInformationSchema;

import com.foundationdb.util.Strings;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.io.File;
import java.io.IOException;

@RunWith(NamedParameterizedRunner.class)
public class DPhypEnumerateTest extends OptimizerTestBase 
                                implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "enum-dphyp");

    protected File schemaFile, rulesFile;

    @TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<>();
        File schemaFile = new File(RESOURCE_DIR, "schema.ddl");
        File rulesFile = new File(RESOURCE_DIR, "rules.yml");
        for (Object[] args : sqlAndExpected(RESOURCE_DIR)) {
            Object[] nargs = new Object[args.length+2];
            nargs[0] = args[0];
            nargs[1] = schemaFile;
            nargs[2] = rulesFile;
            System.arraycopy(args, 1, nargs, 3, args.length-1);
            result.add(nargs);
        }
        return NamedParamsTestBase.namedCases(result);
    }

    public DPhypEnumerateTest(String caseName, File schemaFile, File rulesFile,
                              String sql, String expected, String error) {
        super(caseName, sql, expected, error);
        this.schemaFile = schemaFile;
        this.rulesFile = rulesFile;
    }

    protected RulesContext rules;

    @Before
    public void loadDDL() throws Exception {
        AkibanInformationSchema ais = loadSchema(schemaFile);
        rules = RulesTestContext.create(ais, null, false,
                                        RulesTestHelper.loadRules(rulesFile),
                                        new Properties());
    }

    @Test
    public void testEnumerate() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        PlanContext plan = new PlanContext(rules, 
                                           new AST((DMLStatementNode)stmt,
                                                   parser.getParameterList()));
        rules.applyRules(plan);
        PlanNode node = plan.getPlan();
        Joinable joins = null;
        ConditionList whereConditions = null;
        while (true) {
            if (node instanceof Joinable) {
                joins = ((Joinable)node);
                break;
            }
            if (node instanceof Select)
                whereConditions = ((Select)node).getConditions();
            if (node instanceof BasePlanWithInput)
                node = ((BasePlanWithInput)node).getInput();
            else
                break;
        }
        if (joins == null)
            return null;
        String result = Strings.join(new DPhypEnumerate().run(joins, whereConditions));
        result = result.replace("\r", "");
        result = result.replace(DEFAULT_SCHEMA + ".", "");
        return result;
    }

    @Override
    public void checkResult(String result) throws IOException {
        assertEquals(caseName, expected, result);
    }

    static class DPhypEnumerate extends DPhyp<List<String>> {
        public List<String> evaluateTable(long s, Joinable table) {
            return Collections.singletonList(((ColumnSource)table).getName());
        }

        public List<String> evaluateJoin(long s1, List<String> p1, long s2, List<String> p2, long s, List<String> existing, 
                                         JoinType joinType, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            if (existing == null)
                existing = new ArrayList<>();
            String jstr = " " + joinType + " JOIN ";
            StringBuilder cstr = new StringBuilder(" ON ");
            boolean first = true;
            for (JoinOperator join : joins) {
                if (join.getJoinConditions() != null) {
                    for (ConditionExpression condition : join.getJoinConditions()) {
                        if (first)
                            first = false;
                        else
                            cstr.append(" AND ");
                        cstr.append(condition);
                    }
                }
            }
            if (first) {
                jstr = " CROSS JOIN ";
                cstr.setLength(0);
            }
            for (String left : p1) {
                if (left.indexOf(' ') > 0)
                    left = "(" + left + ")";
                for (String right : p2) {
                    if (right.indexOf(' ') > 0)
                        right = "(" + right + ")";
                    existing.add(left + jstr + right + cstr);
                }
            }
            return existing;
        }
    }
}
