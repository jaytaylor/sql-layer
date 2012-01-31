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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.plan.AST;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.ExpressionVisitor;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.util.AssertUtils;
import com.akiban.util.Strings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.akiban.util.Strings.stripr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(NamedParameterizedRunner.class)
public final class ColumnEquivalenceTest extends OptimizerTestBase {

    public static final File RESOURCE_BASE_DIR =
            new File(OptimizerTestBase.RESOURCE_DIR, "rule");
    public static final File TESTS_RESOURCE_DIR = new File(RESOURCE_BASE_DIR, "column-equivalence");
    
    @TestParameters
    public static Collection<Parameterization> params() throws IOException {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        File schema = new File(TESTS_RESOURCE_DIR, "schema.ddl");
        for (File testFile : TESTS_RESOURCE_DIR.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile() && pathname.getName().endsWith(".test");
            }
        })) {
            List<String> testLines = Strings.dumpFile(testFile);
            Iterator<String> testLinesIter = testLines.iterator();
            String sql = testLinesIter.next();
            Set<Set<String>> columnEquivalenceSets = new HashSet<Set<String>>();
            while (testLinesIter.hasNext()) {
                String columnEquivalenceLine = testLinesIter.next();
                Set<String> columnEquivalences = new HashSet<String>();
                Collections.addAll(columnEquivalences, readEquivalences(columnEquivalenceLine));
                columnEquivalenceSets.add(columnEquivalences);
            }
            pb.add(stripr(testFile.getName(), ".test"), schema, sql,  columnEquivalenceSets);
        }
        
        return pb.asList();
    }

    private static String[] readEquivalences(String columnEquivalenceLine) {
        String[] results = columnEquivalenceLine.trim().split("\\s+");
        for (int i = 0; i < results.length; ++i) {
            String elem = results[i];
            if (elem.split("\\.").length == 2) {
                elem = DEFAULT_SCHEMA + '.' + elem;
                results[i] = elem;
            }
        }
        return results;
    }

    @Before
    public void loadDDL() throws Exception {
        AkibanInformationSchema ais = loadSchema(schemaFile);
        int columnEquivalenceRuleIndex = -1;
        for (int i = 0, max = DefaultRules.DEFAULT_RULES.size(); i < max; i++) {
            BaseRule rule = DefaultRules.DEFAULT_RULES.get(i);
            if (rule instanceof ColumnEquivalenceFinder) {
                columnEquivalenceRuleIndex = i;
                break;
            }
        }
        if (columnEquivalenceRuleIndex < 0)
            throw new RuntimeException(ColumnEquivalenceFinder.class.getSimpleName() + " not found");
        List<BaseRule> rulesSublist = DefaultRules.DEFAULT_RULES.subList(0, columnEquivalenceRuleIndex + 1);
        rules = new RulesTestContext(ais, DEFAULT_SCHEMA, null,
                rulesSublist,
                new Properties());
    }

    @Test
    public void test() throws Exception {
        Set<Set<String>> actualEquivalentColumns = getActualEquivalentColumns();
        AssertUtils.assertCollectionEquals("for [ " + sql + " ]: ", equivalences, actualEquivalentColumns);
    }

    private Set<Set<String>> getActualEquivalentColumns() throws Exception {
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

        Set<Set<ColumnExpression>> set = new HashSet<Set<ColumnExpression>>();
        List<ColumnExpression> columnExpressions = new ColumnFinder().find(plan.getPlan());
        for (ColumnExpression columnExpression : columnExpressions) {
            Set<ColumnExpression> belongsToSet = null;
            for (Set<ColumnExpression> equivalentExpressions : set) {
                Iterator<ColumnExpression> equivalentIters = equivalentExpressions.iterator();
                boolean isInSet = equivalentIters.next().equivalentTo(columnExpression);
                // as a sanity check, ensure that this is consistent for the rest of them
                while (equivalentIters.hasNext()) {
                    ColumnExpression next = equivalentIters.next();
                    assertEquals(
                            "equivalence for " + columnExpression + " against " + next + " in " + equivalentExpressions,
                            isInSet,
                            next.equivalentTo(columnExpression) && columnExpression.equivalentTo(next)
                    );
                }
                if (isInSet) {
                    assertNull(columnExpression + " already in set: " + belongsToSet, belongsToSet);
                    belongsToSet = equivalentExpressions;
                }
            }
            if (belongsToSet == null) {
                belongsToSet = new HashSet<ColumnExpression>();
                set.add(belongsToSet);
            }
            belongsToSet.add(columnExpression);
        }
        
        Set<Set<String>> stringSets = new HashSet<Set<String>>();
        for (Set<ColumnExpression> equivalenceSet : set) {
            Set<String> stringSet = new HashSet<String>();
            for (ColumnExpression columnExpression : equivalenceSet) {
                stringSet.add(String.valueOf(columnExpression));
            }
            stringSets.add(stringSet);
        }
        return stringSets;
    }

    public ColumnEquivalenceTest(File schemaFile, String sql, Set<Set<String>> equivalences) {
        super(sql, sql, null, null);
        this.equivalences = equivalences;
        this.schemaFile = schemaFile;
    }
    
    private File schemaFile;
    private Set<Set<String>> equivalences;
    private RulesContext rules;
    
    private static class ColumnFinder implements PlanVisitor, ExpressionVisitor {
        List<ColumnExpression> result = new ArrayList<ColumnExpression>();

        public List<ColumnExpression> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression)
                result.add((ColumnExpression)n);
            return true;
        }
    }
}
