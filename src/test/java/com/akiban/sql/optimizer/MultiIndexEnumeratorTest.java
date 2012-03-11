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
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.sql.optimizer.plan.MultiIndexCandidateBase;
import com.akiban.sql.optimizer.plan.MultiIndexEnumerator;
import com.akiban.util.AssertUtils;
import com.akiban.util.Strings;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            pb.add(yaml.getName(), yaml, ddl);
        }
        return pb.asList();
    }
    
    @Test
    public void test() throws IOException {
        SchemaFactory factory = new SchemaFactory(DEFAULT_SCHEMA);
        AkibanInformationSchema ais = factory.ais(ddl);

        TestCase tc = (TestCase) new Yaml().load(new FileReader(yaml));
        List<Index> indexes = allIndexes(ais, tc.getUsingIndexes());
        Set<String> conditions = new HashSet<String>(tc.getConditionsOn());
        Collection<MultiIndexEnumerator.MultiIndexPair<String>> enumerated = enumerator.get(indexes, conditions);
        List<Combination> actual = new ArrayList<Combination>(enumerated.size());
        for (MultiIndexEnumerator.MultiIndexPair<String> elem : enumerated) {
            Combination combo = new Combination();
            
            MultiIndexCandidateBase<String> output = elem.getOutputIndex();
            combo.setOutputIndex(output.getIndex());
            combo.setOutputSkip(output.getPegged().size());

            MultiIndexCandidateBase<String> selector = elem.getSelectorIndex();
            combo.setSelectorIndex(selector.getIndex());
            combo.setSelectorSkip(selector.getPegged().size());

            actual.add(combo);
        }
        AssertUtils.assertCollectionEquals("enumerations", tc.getCombinations(), actual);
    }

    private List<Index> allIndexes(AkibanInformationSchema ais, Set<String> usingIndexes) {
        List<Index> results = new ArrayList<Index>();
        for (Group group : ais.getGroups().values()) {
            addIndexes(group.getIndexes(), results, usingIndexes);
            tableIndexes(group.getGroupTable().getRoot(), results, usingIndexes);
        }
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
            if (filter.contains(indexName))
                output.add(index);
        }
    }

    private static String indexToString(Index index) {
        return String.format("%s.%s",
                index.leafMostTable().getName().getTableName(),
                index.getIndexName().getName()
        );
    }

    public MultiIndexEnumeratorTest(File yaml, String[] ddl) {
        this.yaml = yaml;
        this.ddl = ddl;
    }
    
    private File yaml;
    private String[] ddl;
    
    private static MultiIndexEnumerator<String> enumerator = new MultiIndexEnumerator<String>() {
        @Override
        protected MultiIndexCandidateBase<String> createSeedCandidate(Index index, Set<String> conditions) {
            return new MultiIndexCandidateBase<String>(index, conditions) {
                @Override
                protected boolean columnsMatch(String condition, Column column) {
                    return condition.equals(String.valueOf(column));
                }
            };
        }
    };

    @SuppressWarnings("unused") // getters and setters used by yaml reflection
    public static class TestCase {
        public Set<String> usingIndexes;
        public Set<String> conditionsOn;
        public List<Combination> combinations;

        public Set<String> getConditionsOn() {
            return conditionsOn;
        }

        public List<Combination> getCombinations() {
            return combinations;
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

        public void setCombinations(List<Combination> combinations) {
            this.combinations = new ArrayList<Combination>(combinations);
            Collections.sort(this.combinations);
        }

        @Override
        public String toString() {
            return String.format("conditions: %s, combinations: %s", conditionsOn, combinations);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestCase testCase = (TestCase) o;

            return combinations.equals(testCase.combinations) && conditionsOn.equals(testCase.conditionsOn);
        }

        @Override
        public int hashCode() {
            int result = conditionsOn.hashCode();
            result = 31 * result + combinations.hashCode();
            return result;
        }
    }

    @SuppressWarnings("unused") // getters and setters used by yaml reflection
    public static class Combination implements Comparable<Combination> {
        public String outputIndex;
        public String selectorIndex;
        public int outputSkip;
        public int selectorSkip;

        public String getOutputIndex() {
            return outputIndex;
        }
        
        public void setOutputIndex(Index index) {
            setOutputIndex(indexToString(index));
        }

        public void setOutputIndex(String outputIndex) {
            this.outputIndex = outputIndex;
        }

        public String getSelectorIndex() {
            return selectorIndex;
        }

        public void setSelectorIndex(Index index) {
            setSelectorIndex(indexToString(index));
        }

        public void setSelectorIndex(String selectorIndex) {
            this.selectorIndex = selectorIndex;
        }

        public int getOutputSkip() {
            return outputSkip;
        }

        public void setOutputSkip(int outputSkip) {
            this.outputSkip = outputSkip;
        }

        public int getSelectorSkip() {
            return selectorSkip;
        }

        public void setSelectorSkip(int selectorSkip) {
            this.selectorSkip = selectorSkip;
        }

        @Override
        public String toString() {
            return String.format("(output <skip %d from %s> selector: <skip %d from %s>)",
                    getOutputSkip(),
                    getOutputIndex(),
                    getSelectorSkip(),
                    getSelectorIndex()
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Combination that = (Combination) o;

            return outputSkip == that.outputSkip && selectorSkip == that.selectorSkip
                    && outputIndex.equals(that.outputIndex)
                    && selectorIndex.equals(that.selectorIndex);
        }

        @Override
        public int hashCode() {
            int result = outputIndex.hashCode();
            result = 31 * result + selectorIndex.hashCode();
            result = 31 * result + outputSkip;
            result = 31 * result + selectorSkip;
            return result;
        }

        @Override
        public int compareTo(Combination o) {
            int cmp = getOutputIndex().compareTo(o.getOutputIndex());
            if (cmp != 0)
                return cmp;
            cmp = getSelectorIndex().compareTo(o.getSelectorIndex());
            if (cmp != 0)
                return cmp;
            cmp = getOutputSkip() - o.getOutputSkip();
            if (cmp != 0)
                return cmp;
            cmp = getSelectorSkip() - o.getSelectorSkip();
            return cmp;
        }
    }
}
