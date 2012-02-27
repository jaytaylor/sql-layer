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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

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
            Map<Set<Map<String,Boolean>>,Integer> tmp = new HashMap<Set<Map<String, Boolean>>, Integer>();
            Set<Map<String,Boolean>> columnEquivalenceSets = new HashSet<Map<String, Boolean>>();
            while (testLinesIter.hasNext()) {
                String columnEquivalenceLine = testLinesIter.next();
                Map<String,Boolean> columnEquivalences = new HashMap<String, Boolean>();
                String[] columnNames = readEquivalences(columnEquivalenceLine);
                for (String columnName : columnNames)
                    columnEquivalences.put(columnName, columnNames.length == 1);
                columnEquivalenceSets.add(columnEquivalences);
            }
            tmp.put(columnEquivalenceSets, 0); // TODO remove before checking in!
            pb.add(stripr(testFile.getName(), ".test"), schema, sql,  tmp);
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
    public void equivalences() throws Exception {
        Map<Set<Map<String,Boolean>>,Integer> actualEquivalentColumns = getActualEquivalentColumns();
        AssertUtils.assertMapEquals("for [ " + sql + " ]: ", equivalences, actualEquivalentColumns);
    }

    private Map<Set<Map<String,Boolean>>,Integer> getActualEquivalentColumns() throws Exception {
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

        Map<Set<Map<String,Boolean>>,Integer> result = new HashMap<Set<Map<String, Boolean>>, Integer>();
        Map<Collection<ColumnExpression>,Integer> columnExpressionsToDepth = new ColumnFinder().find(plan.getPlan());
        for (Map.Entry<Collection<ColumnExpression>,Integer> entry : columnExpressionsToDepth.entrySet()) {
            Collection<ColumnExpression> columnExpressions = entry.getKey();
            Integer depth = entry.getValue();
            Set<Map<String, Boolean>> byName = collectEquivalentColumns(columnExpressions);
            Object old = result.put(byName, depth);
            assertNull("bumped: " + old, old);
        }
        return result;
    }

    private Set<Map<String, Boolean>> collectEquivalentColumns(Collection<ColumnExpression> columnExpressions) {
        Set<Set<ColumnExpression>> set = new HashSet<Set<ColumnExpression>>();
        for (ColumnExpression columnExpression : columnExpressions) {
            Set<ColumnExpression> belongsToSet = null;
            for (Set<ColumnExpression> equivalentExpressions : set) {
                Iterator<ColumnExpression> equivalentIters = equivalentExpressions.iterator();
                boolean isInSet = areEquivalent(equivalentIters.next(), columnExpression);
                // as a sanity check, ensure that this is consistent for the rest of them
                while (equivalentIters.hasNext()) {
                    ColumnExpression next = equivalentIters.next();
                    assertEquals(
                            "equivalence for " + columnExpression + " against " + next + " in " + equivalentExpressions,
                            isInSet,
                            areEquivalent(next, columnExpression) && areEquivalent(columnExpression, next)
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

        Set<Map<String,Boolean>> byName = new HashSet<Map<String, Boolean>>();
        for (Set<ColumnExpression> equivalenceSet : set) {
            Map<String,Boolean> nameAndNullability = new TreeMap<String, Boolean>();
            for (ColumnExpression columnExpression : equivalenceSet) {
                nameAndNullability.put(String.valueOf(columnExpression), columnExpression.getSQLtype().isNullable());
            }
            byName.add(nameAndNullability);
        }
        return byName;
    }

    private static boolean areEquivalent(ColumnExpression one, ColumnExpression two) {
        return one.getEquivalenceFinder().areEquivalent(one, two) && two.getEquivalenceFinder().areEquivalent(two, one);
    }

    public ColumnEquivalenceTest(File schemaFile, String sql, Map<Set<Map<String,Boolean>>,Integer> equivalences) {
        super(sql, sql, null, null);
        this.equivalences = equivalences;
        this.schemaFile = schemaFile;
    }
    
    private File schemaFile;
    private Map<Set<Map<String,Boolean>>,Integer> equivalences;
    private RulesContext rules;
    
    private static class ColumnFinder implements PlanVisitor, ExpressionVisitor {
        List<ColumnExpression> result = new ArrayList<ColumnExpression>();

        public Map<Collection<ColumnExpression>,Integer> find(PlanNode root) {
            root.accept(this);
            Map<Collection<ColumnExpression>,Integer> tmp = new HashMap<Collection<ColumnExpression>, Integer>(); // TODO remove this before checkin!
            tmp.put(result, 0);
            return tmp;
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
