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

package com.akiban.sql.optimizer;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.sql.optimizer.plan.IndexIntersectionNode;
import com.akiban.sql.optimizer.plan.MultiIndexCandidate;
import com.akiban.sql.optimizer.plan.MultiIndexEnumerator;
import com.akiban.sql.optimizer.plan.MultiIndexEnumerator.BranchInfo;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;
import com.akiban.util.AssertUtils;
import com.akiban.util.Strings;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(NamedParameterizedRunner.class)
public final class MultiIndexEnumeratorTest {
    
    private static final File TEST_DIR = new File(OptimizerTestBase.RESOURCE_DIR, "multi-index-enumeration");
    private static final File SCHEMA_DIR = new File(TEST_DIR, "schema.ddl");
    private static final String DEFAULT_SCHEMA = "mie";
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() throws IOException{
        System.out.println(SCHEMA_DIR.getAbsolutePath());
        List<String> ddlList = Strings.dumpFile(SCHEMA_DIR);
        String[] ddl = ddlList.toArray(new String[ddlList.size()]);
        File[] yamls = TEST_DIR.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".yaml");
            }
        });
        ParameterizationBuilder pb = new ParameterizationBuilder();
        for (File yaml : yamls) {
            String name = yaml.getName();
            name = name.substring(0, name.length() - ".yaml".length());
            pb.add(name, yaml, ddl);
        }
        return pb.asList();
    }
    
    @Test @OnlyIfNot("expectException")
    public void combinations() throws IOException {
        Collection<TestNode> enumerated = getEnumerations();
        Set<String> actual = new TreeSet<String>(); 
        for (IndexIntersectionNode elem : enumerated) {
            actual.add(elem.toString());
        }
        Set<String> expected = tc.getResults();
        AssertUtils.assertCollectionEquals("enumerations", expected, actual);
    }

    @Test(expected = DuplicateConditionException.class) @OnlyIf("expectException")
    public void expectError() {
        getEnumerations();
    }

    private Collection<TestNode> getEnumerations() {
        SchemaFactory factory = new SchemaFactory(DEFAULT_SCHEMA);
        AkibanInformationSchema ais = factory.ais(ddl);
        factory.rowDefCache(ais); // set up indx row compositions

        List<Index> indexes = allIndexes(ais, tc.getUsingIndexes());
        Collection<Set<Condition>> conditions = branchConditions(tc.getConditionsOn(), ais);
        EquivalenceFinder<Column> columnEquivalences = innerJoinEquivalencies(ais);
        addExtraEquivalencies(tc.getExtraEquivalencies(), ais, columnEquivalences);

        StringConditionEnumerator enumerator = new StringConditionEnumerator();
        for (Set<Condition> branch : conditions) {
            StringBranchInfo info = new StringBranchInfo(ais, indexes, branch);
            enumerator.addBranch(info);
        }

        return enumerator.getCombinations(columnEquivalences);
    }

    private Collection<Set<Condition>> branchConditions(Set<String> conditionsOn, AkibanInformationSchema ais) {
        Map<String,Set<Condition>> conditionsByBranch = new HashMap<String, Set<Condition>>(conditionsOn.size());
        for (String qualified : conditionsOn) {
            Condition cond = new Condition(qualified, ais);
            Set<Condition> conditions = conditionsByBranch.get(cond.branch);
            if (conditions == null) {
                conditions = new HashSet<Condition>();
                conditionsByBranch.put(cond.branch, conditions);
            }
            if (!conditions.add(cond))
                throw new RuntimeException("duplicate condition: " + cond);
        }
        return conditionsByBranch.values();
    }

    private void addExtraEquivalencies(Map<String, String> equivMap, AkibanInformationSchema ais,
                                       EquivalenceFinder<Column> output)
    {
        for (Map.Entry<String,String> entry : equivMap.entrySet()) {
            String one = entry.getKey();
            String two = entry.getValue();
            Column oneCol = new Condition(one, ais).aisColumn();
            Column twoCol = new Condition(two, ais).aisColumn();
            output.markEquivalent(oneCol, twoCol);
        }
    }
    
    private static class Condition {
        private static final String DEFAULT_BRANCH = "";
        private static final Pattern PATTERN = Pattern.compile("(?:\\((\\w+)(?:\\.(\\w+))?\\))?\\s*(\\w+)\\.(\\w+)");
        private String branch;
        private String condId; // local to each branch
        private String table;
        private String column;
        private AkibanInformationSchema ais;
        
        private Condition(String qualfiedString, AkibanInformationSchema ais) {
            Matcher matcher = PATTERN.matcher(qualfiedString);
            if (!matcher.find())
                throw new RuntimeException("illegal condition specifier: " + qualfiedString);
            branch = matcher.group(1);
            condId = matcher.group(2);
            table = matcher.group(3);
            column = matcher.group(4);
            if (branch == null || branch.length() == 0)
                branch = DEFAULT_BRANCH;
            if (condId == null || condId.length() == 0)
                condId = DEFAULT_BRANCH;
            this.ais = ais;
        }
        
        public Column aisColumn() {
            UserTable uTable = ais.getUserTable(DEFAULT_SCHEMA, table);
            if (uTable == null)
                throw new RuntimeException("table not found: " + table);
            Column aisColumn = uTable.getColumn(column);
            if (aisColumn == null)
                throw new RuntimeException("column not found: " + column);
            return aisColumn;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (!DEFAULT_BRANCH.equals(branch)) {
                sb.append('(').append(branch);
                if (!DEFAULT_BRANCH.equals(condId))
                    sb.append('.').append(condId);
                sb.append(") ");
            }
            sb.append(table).append('.').append(column);
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Condition condition = (Condition) o;

            return branch.equals(condition.branch) && condId.equals(condition.condId)
            && column.equals(condition.column) && table.equals(condition.table);

        }

        @Override
        public int hashCode() {
            int result = branch.hashCode();
            result = 31 * result + branch.hashCode();
            result = 31 * result + table.hashCode();
            result = 31 * result + column.hashCode();
            return result;
        }
    }

    private EquivalenceFinder<Column> innerJoinEquivalencies(AkibanInformationSchema ais) {
        EquivalenceFinder<Column> columnEquivalences = new EquivalenceFinder<Column>();
        for (Group group : ais.getGroups().values()) {
            buildInnerJoinEquivalencies(group.getGroupTable().getRoot(), columnEquivalences);
        }
        return columnEquivalences;
    }

    private void buildInnerJoinEquivalencies(UserTable table, EquivalenceFinder<Column> equivalences) {
        for (Join join : table.getChildJoins()) {
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                equivalences.markEquivalent(joinColumn.getChild(), joinColumn.getParent());
            }
            buildInnerJoinEquivalencies(join.getChild(), equivalences);
        }
    }

    private List<Index> allIndexes(AkibanInformationSchema ais, Set<String> usingIndexes) {
        List<Index> results = new ArrayList<Index>();
        for (Group group : ais.getGroups().values()) {
            addIndexes(group.getIndexes(), results, usingIndexes);
            tableIndexes(group.getGroupTable().getRoot(), results, usingIndexes);
        }
        if (!usingIndexes.isEmpty())
            throw new RuntimeException("unknown index(es): " + usingIndexes);
        return results;
    }
    
    private void tableIndexes(UserTable table, List<Index> output, Set<String> usingIndexes) {
        addIndexes(table.getIndexesIncludingInternal(), output, usingIndexes);
        for (Join join : table.getChildJoins()) {
            UserTable child = join.getChild();
            tableIndexes(child, output, usingIndexes);
        }
    }

    private void addIndexes(Collection<? extends Index> indexes, List<Index> output, Set<String> filter) {
        for (Index index : indexes) {
            String indexName = indexToString(index);
            if (filter.remove(indexName))
                output.add(index);
        }
    }

    private static String indexToString(Index index) {
        return String.format("%s.%s",
                index.leafMostTable().getName().getTableName(),
                index.getIndexName().getName()
        );
    }

    public MultiIndexEnumeratorTest(File yaml, String[] ddl) throws IOException {
        tc = (TestCase) new Yaml().load(new FileReader(yaml));
        this.ddl = ddl;
        this.expectException = tc.isError();
    }
    
    private String[] ddl;
    private TestCase tc;
    public final boolean expectException;
    
    private static class StringBranchInfo implements BranchInfo<Condition> {
        List<Index> indexes;
        Set<Condition> conditions;
        AkibanInformationSchema ais;

        private StringBranchInfo(AkibanInformationSchema ais, List<Index> indexes, Set<Condition> conditions) {
            this.indexes = indexes;
            this.conditions = conditions;
            this.ais = ais;
        }

        @Override
        public Column columnFromCondition(Condition condition) {
            return condition.aisColumn();
        }

        @Override
        public Collection<? extends Index> getIndexes() {
            return indexes;
        }

        @Override
        public Set<Condition> getConditions() {
            return conditions;
        }
    }
    
    private static class StringConditionEnumerator extends MultiIndexEnumerator<Condition,StringBranchInfo,TestNode> {

        @Override
        protected void handleDuplicateCondition() {
            throw new DuplicateConditionException();
        }

        @Override
        protected TestNode buildLeaf(MultiIndexCandidate<Condition> candidate, StringBranchInfo branchInfo) {
            Index index = candidate.getIndex();
            UserTable leaf = (UserTable) index.leafMostTable();
            return new SimpleLeaf(leaf, index.getAllColumns(), candidate.getPegged());
        }

        @Override
        protected List<Column> getComparisonColumns(TestNode first, TestNode second, EquivalenceFinder<Column> equivs) {
            List<IndexColumn> firstUnpegged = first.getUnpegged();
            List<IndexColumn> secondUnpegged = second.getUnpegged();
            int ncols = Math.min(firstUnpegged.size(), secondUnpegged.size());
            List<Column> results = new ArrayList<Column>(ncols);
            for (int i=0; i < ncols; ++i) {
                Column firstCol = firstUnpegged.get(i).getColumn();
                Column secondCol = secondUnpegged.get(i).getColumn();
                if (!equivs.areEquivalent(firstCol, secondCol))
                    break;
                results.add(firstCol);
            }
            return results;
        }

        @Override
        protected TestNode intersect(TestNode first, TestNode second,
                                                  int comparisonCount) {
            return new SimpleBranch(first, second, comparisonCount);
        }
    }
    
    private interface TestNode extends IndexIntersectionNode<Condition,TestNode> {
        String leafBranch();
        List<IndexColumn> getUnpegged();
    }
    
    private static class SimpleLeaf implements TestNode {
        private UserTable leaf;
        private List<IndexColumn> allCols;
        private List<Condition> pegged;
        private String leafBranch;

        private SimpleLeaf(UserTable leaf, List<IndexColumn> allCols, List<Condition> pegged) {
            this.leaf = leaf;
            this.allCols = allCols;
            this.pegged = pegged;
            for (Condition cond : pegged) {
                if (leafBranch == null)
                    leafBranch = cond.branch;
                else if (!leafBranch.equals(cond.branch))
                    throw new RuntimeException("mis-created branch: " + pegged);
            }
        }

        @Override
        public List<IndexColumn> getUnpegged() {
            return allCols.subList(pegged.size(), allCols.size());
        }

        @Override
        public UserTable getLeafMostUTable() {
            return leaf;
        }

        @Override
        public List<IndexColumn> getAllColumns() {
            return allCols;
        }

        @Override
        public int getPeggedCount() {
            return pegged.size();
        }

        @Override
        public UserTable findCommonAncestor(TestNode other) {
            List<UserTable> myAncestors = leaf.getAncestors();
            List<UserTable> otherAncestors = other.getLeafMostUTable().getAncestors();
            if (myAncestors.size() > 2 || otherAncestors.size() > 2)
                throw new RuntimeException("depth must be <= 2: " + myAncestors + ", " + otherAncestors);
            int common;
            int minTables = Math.min(myAncestors.size(), otherAncestors.size());
            for (common = 0; common < minTables; ++common) {
                UserTable myAncestor = myAncestors.get(common);
                UserTable otherAncestor = otherAncestors.get(common);
                if (myAncestor != otherAncestor)
                    break;
            }
            assert common > 0 : "none in common between " + this + " and " + other;
            return myAncestors.get(common-1);
        }

        @Override
        public boolean removeCoveredConditions(Collection<? super Condition> conditions, Collection<? super Condition> removeTo) {
            boolean removedAny = false;
            for (Condition cond : pegged) {
                if(conditions.remove(cond)) {
                    removeTo.add(cond);
                    removedAny = true;
                }
            }
            return removedAny;
        }

        @Override
        public String leafBranch() {
            return leafBranch;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            int npegged = pegged.size();
            for (int i = 0, allColsSize = allCols.size(); i < allColsSize; i++) {
                IndexColumn icol = allCols.get(i);
                String colName = icol.getColumn().getName();
                if (i < npegged) {
                    Condition cond = pegged.get(i);
                    if (!Condition.DEFAULT_BRANCH.equals(cond.branch))
                        sb.append(cond.branch).append('.');
                    colName = colName.toUpperCase();
                }
                sb.append(colName);
                if (i < allColsSize-1)
                    sb.append(", ");
                else
                sb.append(']');
            }
            return sb.toString();
        }
        
        
    }
    
    private static class SimpleBranch implements TestNode {
        TestNode left, right;
        int comparisons;

        private SimpleBranch(TestNode left, TestNode right, int comparisons) {
            this.left = left;
            this.right = right;
            this.comparisons = comparisons;
        }

        @Override
        public List<IndexColumn> getUnpegged() {
            return left.getUnpegged();
        }

        @Override
        public UserTable getLeafMostUTable() {
            return left.getLeafMostUTable();
        }

        @Override
        public List<IndexColumn> getAllColumns() {
            return left.getAllColumns();
        }

        @Override
        public int getPeggedCount() {
            return left.getPeggedCount();
        }

        @Override
        public UserTable findCommonAncestor(TestNode other) {
            return left.findCommonAncestor(other);
        }

        @Override
        public boolean removeCoveredConditions(Collection<? super Condition> conditions, Collection<? super Condition> removeTo) {
            // using a bitwise or on purpose here -- we do NOT want to short-circuit this, since even if the left
            // covers some conditions, we want to know which ones the right covers.
            return left.removeCoveredConditions(conditions, removeTo)
                    | right.removeCoveredConditions(conditions, removeTo);
        }

        @Override
        public String toString() {
            return String.format("INTERSECT( %s AND %s with %d comparison%s)",
                    left.toString(), right.toString(), comparisons, (comparisons == 1 ? "" : "s"));
        }

        @Override
        public String leafBranch() {
            return left.leafBranch();
        }
    }
    
    private static class DuplicateConditionException extends RuntimeException {}

    @SuppressWarnings("unused") // getters and setters used by yaml reflection
    public static class TestCase {
        public Set<String> usingIndexes;
        public Set<String> conditionsOn;
        public Set<String> results;
        public Map<String,String> extraEquivalencies = Collections.emptyMap();
        public boolean isError = false;

        public void setError() {
            isError = true;
        }

        public boolean isError() {
            return isError;
        }
        
        public Set<String> getConditionsOn() {
            return conditionsOn;
        }

        public Set<String> getResults() {
            if (!(results instanceof TreeSet))
                results = new TreeSet<String>(results);
            return results;
        }

        public void setConditionsOn(Set<String> conditionsOn) {
            this.conditionsOn = conditionsOn;
        }
        
        public void setUsingIndexes(Set<String> usingIndexes) {
            this.usingIndexes = usingIndexes;
        }
        
        public Set<String> getUsingIndexes() {
            return usingIndexes;
        }

        public Map<String, String> getExtraEquivalencies() {
            return extraEquivalencies;
        }

        public void setExtraEquivalencies(Map<String, String> extraEquivalences) {
            this.extraEquivalencies = extraEquivalences;
        }

        public void setCombinations(List<String> results) {
            this.results = new TreeSet<String>(results);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("conditions: ").append(conditionsOn).append(", ");
            if (isError)
                sb.append("isError=true");
            else
                sb.append("combinations: ").append(results);
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestCase testCase = (TestCase) o;

            return results.equals(testCase.results)
                    && conditionsOn.equals(testCase.conditionsOn)
                    && isError == testCase.isError;
        }

        @Override
        public int hashCode() {
            int result = conditionsOn.hashCode();
            result = 31 * result + results.hashCode();
            return result;
        }
    }
}
