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
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.optimizer.rule.PlanContext;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.util.AssertUtils;
import com.akiban.util.Strings;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.akiban.util.AssertUtils.assertCollectionEquals;
import static com.akiban.util.Strings.stripr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class ColumnEquivalenceTest extends OptimizerTestBase {

    public static final File RESOURCE_BASE_DIR =
            new File(OptimizerTestBase.RESOURCE_DIR, "rule");
    public static final File TESTS_RESOURCE_DIR = new File(RESOURCE_BASE_DIR, "column-equivalence");
    private static final Pattern SUBQUERY_DEPTH_PATTERN = Pattern.compile(
            "--\\s*subquery\\s+at\\s+depth\\s+(\\d+):",
            Pattern.CASE_INSENSITIVE
    );
    
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
            int depth = 0;
            while (testLinesIter.hasNext()) {
                String columnEquivalenceLine = testLinesIter.next().trim();
                Matcher depthMatcher = SUBQUERY_DEPTH_PATTERN.matcher(columnEquivalenceLine);
                if (depthMatcher.matches()) {
                    tmp.put(columnEquivalenceSets, depth);
                    depth = Integer.parseInt(depthMatcher.group(1));
                    columnEquivalenceSets = new HashSet<Map<String, Boolean>>();
                    continue;
                }
                Map<String,Boolean> columnEquivalences = new HashMap<String, Boolean>();
                String[] columnNames = readEquivalences(columnEquivalenceLine);
                for (String columnName : columnNames)
                    columnEquivalences.put(columnName, columnNames.length == 1);
                columnEquivalenceSets.add(columnEquivalences);
            }
            tmp.put(columnEquivalenceSets, depth);
            pb.add(stripr(testFile.getName(), ".test"), schema, sql,  tmp);
        }
        
        return pb.asList();
    }

    private static String[] readEquivalences(String columnEquivalenceLine) {
        return columnEquivalenceLine.split("\\s+");
    }

    @Before
    public void loadDDL() throws Exception {
        AkibanInformationSchema ais = loadSchema(schemaFile);
        int columnEquivalenceRuleIndex = -1;
        for (int i = 0, max = DefaultRules.DEFAULT_RULES_CBO.size(); i < max; i++) {
            BaseRule rule = DefaultRules.DEFAULT_RULES_CBO.get(i);
            if (rule instanceof ColumnEquivalenceFinder) {
                columnEquivalenceRuleIndex = i;
                break;
            }
        }
        if (columnEquivalenceRuleIndex < 0)
            throw new RuntimeException(ColumnEquivalenceFinder.class.getSimpleName() + " not found");
        List<BaseRule> rulesSublist = DefaultRules.DEFAULT_RULES_CBO.subList(0, columnEquivalenceRuleIndex + 1);
        rules = RulesTestContext.create(ais, null, false, rulesSublist, new Properties());
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
        Collection<EquivalenceScope> scopes = new ColumnFinder().find(plan.getPlan());
        for (EquivalenceScope scope : scopes) {
            Collection<ColumnExpression> columnExpressions = scope.columns;
            int depth = scope.depth;
            Set<Map<String, Boolean>> byName = collectEquivalentColumns(columnExpressions, scope.equivs);
            Object old = result.put(byName, depth);
            assertNull("bumped: " + old, old);
            // anything in the equivs participants must also be in the scope's columns.
            HashSet<ColumnExpression> columnsSet = new HashSet<ColumnExpression>(scope.columns);
            assertEquals("columns in equivalencies", columnsSet, new HashSet<ColumnExpression>(scope.columns));
            Set<ColumnExpression> inScopeParticipants = Sets.intersection(scope.equivs.findParticipants(), columnsSet);
            assertCollectionEquals("columns in equivalencies", inScopeParticipants, scope.equivs.findParticipants());
        }
        return result;
    }

    private Set<Map<String, Boolean>> collectEquivalentColumns(Collection<ColumnExpression> columnExpressions,
                                                               EquivalenceFinder<ColumnExpression> equivs) {
        Set<Set<ColumnExpression>> set = new HashSet<Set<ColumnExpression>>();
        for (ColumnExpression columnExpression : columnExpressions) {
            Set<ColumnExpression> belongsToSet = null;
            for (Set<ColumnExpression> equivalentExpressions : set) {
                Iterator<ColumnExpression> equivalentIters = equivalentExpressions.iterator();
                boolean isInSet = areEquivalent(equivalentIters.next(), columnExpression, equivs);
                // as a sanity check, ensure that this is consistent for the rest of them
                while (equivalentIters.hasNext()) {
                    ColumnExpression next = equivalentIters.next();
                    boolean bothEquivalent = areEquivalent(next, columnExpression, equivs)
                            && areEquivalent(columnExpression, next, equivs);
                    assertEquals(
                            "equivalence for " + columnExpression + " against " + next + " in " + equivalentExpressions,
                            isInSet,
                            bothEquivalent
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

    private static boolean areEquivalent(ColumnExpression one, ColumnExpression two,
                                         EquivalenceFinder<ColumnExpression> equivs) {
        return equivs.areEquivalent(one, two) && equivs.areEquivalent(two, one);
    }

    public ColumnEquivalenceTest(File schemaFile, String sql, Map<Set<Map<String,Boolean>>,Integer> equivalences) {
        super(sql, sql, null, null);
        this.equivalences = equivalences;
        this.schemaFile = schemaFile;
    }
    
    private File schemaFile;
    private Map<Set<Map<String,Boolean>>,Integer> equivalences;
    private RulesContext rules;
    
    private static class EquivalenceScope {
        Collection<ColumnExpression> columns;
        EquivalenceFinder<ColumnExpression> equivs;
        int depth;

        private EquivalenceScope(int depth, Collection<ColumnExpression> columns,
                                 EquivalenceFinder<ColumnExpression> equivs) {
            this.depth = depth;
            this.columns = columns;
            this.equivs = equivs;
        }

        @Override
        public String toString() {
            return String.format("scope(cols=%s, depth=%d, %s", columns, depth, equivs);
        }
    }
    
    private static class ColumnFinder implements PlanVisitor, ExpressionVisitor {
        ColumnEquivalenceStack equivsStack = new ColumnEquivalenceStack();
        Deque<List<ColumnExpression>> columnExpressionsStack = new ArrayDeque<List<ColumnExpression>>();
        Collection<EquivalenceScope> results = new ArrayList<EquivalenceScope>();

        public Collection<EquivalenceScope> find(PlanNode root) {
            root.accept(this);
            assertTrue("stack isn't empty: " + columnExpressionsStack, columnExpressionsStack.isEmpty());
            assertTrue("equivs stack aren't empty", equivsStack.isEmpty());
            return results;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (equivsStack.enterNode(n))
                columnExpressionsStack.push(new ArrayList<ColumnExpression>());
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            EquivalenceFinder<ColumnExpression> nodeEquivs = equivsStack.leaveNode(n);
            if (nodeEquivs != null) {
                Collection<ColumnExpression> collected = columnExpressionsStack.pop();
                int depth = columnExpressionsStack.size();
                EquivalenceScope scope = new EquivalenceScope(depth, collected, nodeEquivs);
                results.add(scope);
            }
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
                columnExpressionsStack.peek().add((ColumnExpression) n);
            return true;
        }
    }
}
