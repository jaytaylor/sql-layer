/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.sql.pg;

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.util.RandomRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Randomly generate 2 queries, then
 * run Query1 & Query2 and store results
 * do
 *   SELECT id FROM (Query1) WHERE id IN (Query2)
 * Manually compute results from stored results,
 * and compare with whole query.
 * Also handles NOT IN and EXISTS and NOT EXISTS, could handle more
 *
 */
@Ignore("Waiting until this passes most of the time")
@RunWith(SelectedParameterizedRunner.class)
public class RandomSemiJoinTestDT extends PostgresServerITBase {

    private static final Logger LOG = LoggerFactory.getLogger(RandomSemiJoinTestDT.class);

    private static final boolean hitPostgres = Boolean.parseBoolean(System.getProperty("fdbsql.test.hit-postgres"));
    private static final int DDL_COUNT = 10;
    private static final int QUERY_COUNT = 30;
    private static final int TABLE_COUNT = 3;
    private static final int COLUMN_COUNT = 10;
    private static final int MAX_ROW_COUNT = 100;
    private static final int MAX_INDEX_COUNT = 5;
    private static final int MAX_CONDITION_COUNT = 10;
    private static final int JOIN_CONSTANT_LIKELYHOOD = 6;
    private static final int WHERE_CONSTANT_LIKELYHOOD = 4;
    private static final int MAX_VALUE = MAX_ROW_COUNT * 2;
    private static final int MIN_VALUE = MAX_ROW_COUNT * -2;
    private static final int MAX_OUTER_LIMIT = 10;

    @ClassRule
    public static final RandomRule randomRule = new RandomRule();
    @Rule
    public final RandomRule testRandom = randomRule;

    /**
     * The seed used for individual parameterized tests, so that they can have different DDL & DML
     */
    private Long testSeed;

    @Override
    protected String getConnectionURL() {
        if (hitPostgres) {
            return "jdbc:postgresql:" + SCHEMA_NAME;
        } else {
            return super.getConnectionURL();
        }
    }

    @Parameterized.Parameters(name="Test Seed: {0}")
    public static List<Object[]> params() throws Exception {
        Random random = randomRule.reset();
        List<Object[]> params = new ArrayList<>(DDL_COUNT);
        for (int i=0; i< DDL_COUNT; i++) {
            params.add(new Object[] {random.nextLong()});
        }
        return params;
    }

    public RandomSemiJoinTestDT(Long testSeed) {

        this.testSeed = testSeed;
    }

    private static String buildQuery(Random random, boolean useExists, boolean firstQuery, TableAliasGenerator tag) {
        if (firstQuery && random.nextInt(20) == 0) {
            return randomTable(random);
        }
        StringBuilder stringBuilder = new StringBuilder();
        int firstTable = tag.createNew();
        String returningSource = "ta" + firstTable + "." + (firstQuery ? "main" : randomColumn(random));
        stringBuilder.append("SELECT ");
        stringBuilder.append(returningSource);
        stringBuilder.append(" FROM ");
        stringBuilder.append(randomTable(random));
        stringBuilder.append(" AS ta");
        stringBuilder.append(firstTable);
        switch (random.nextInt(4)) {
            case 0:
                // Just the FROM
                break;
            case 1:
                addJoinClause("INNER", stringBuilder, random, tag, firstTable);
                break;
            case 2:
                addJoinClause("LEFT OUTER", stringBuilder, random, tag, firstTable);
                break;
            case 3:
                addJoinClause("RIGHT OUTER", stringBuilder, random, tag, firstTable);
                break;
            default:
                throw new IllegalStateException("not enough cases for random values");
        }
        addWhereClause(stringBuilder, random, tag, !firstQuery && useExists, firstTable);
        addLimitClause(stringBuilder, random, returningSource);
        return stringBuilder.toString();
    }

    private static void addLimitClause(StringBuilder stringBuilder, Random random, String returningSource) {
        if (random.nextInt(10) == 0) {
            stringBuilder.append(" ORDER BY ");
            stringBuilder.append(returningSource);
            stringBuilder.append(" LIMIT ");
            stringBuilder.append(random.nextInt(10)+1);
        }
    }

    private static void addWhereClause(StringBuilder stringBuilder, Random random,
                                       TableAliasGenerator tag, boolean forceMainEqualsClause, int firstTable) {
        if (!forceMainEqualsClause && random.nextInt(5) == 0) {
            return;
        }
        stringBuilder.append(" WHERE ");
        addCondition(stringBuilder, random, tag, firstTable, WHERE_CONSTANT_LIKELYHOOD, forceMainEqualsClause);
        for (int i=0; i<MAX_CONDITION_COUNT; i++) {
            if (random.nextInt(5) == 0) {
                break;
            }
            stringBuilder.append(random.nextBoolean() ? " AND " : " OR ");
            addCondition(stringBuilder, random, tag, firstTable, WHERE_CONSTANT_LIKELYHOOD, false);
        }
    }

    private static void addJoinClause(String type, StringBuilder sb, Random random,
                                      TableAliasGenerator tag, int firstTable) {
        sb.append(" ");
        sb.append(type);
        sb.append(" JOIN ");
        sb.append(randomTable(random));
        sb.append(" AS ta");
        sb.append(tag.createNew());
        sb.append(" ON ");
        // no cross joins right now
        int conditionCount = random.nextInt(3);
        for (int i=0; i<conditionCount+1; i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            addCondition(sb, random, tag, firstTable, WHERE_CONSTANT_LIKELYHOOD, false);
        }
    }

    private static void addCondition(StringBuilder sb, Random random, TableAliasGenerator tag,
                                     int firstAvailable, int constantBias, boolean forceMainEqualsClause) {
        int firstTable = tag.randomAbove(firstAvailable);
        int secondTable = tag.randomAbove(firstAvailable, firstTable);
        boolean mainIsFirst = false;
        // 0 => first is constant, 1 => second is constant, else neither
        int oneIsConstant = random.nextInt(constantBias);
        if (oneIsConstant == 0) {
            sb.append(randomValue(random));
        } else {
            mainIsFirst = random.nextBoolean();
            if (mainIsFirst && forceMainEqualsClause) {
                sb.append("%s");
            } else {
                addAliasedSource(sb, random, firstTable);
            }
        }
        int whichComparison = random.nextInt(6);
        switch (whichComparison) {
            case 0:
                sb.append(" < ");
                break;
            case 1:
                sb.append(" > ");
                break;
            default:
                sb.append(" = ");
                break;
        }
        if (oneIsConstant == 1) {
            sb.append(randomValue(random));
        } else {
            if (mainIsFirst || !forceMainEqualsClause) {
                addAliasedSource(sb, random, secondTable);
            } else {
                sb.append("%s");
            }
        }
    }

    private static void addAliasedSource(StringBuilder sb, Random random, int firstTable) {
        sb.append("ta");
        sb.append(firstTable);
        sb.append(".");
        sb.append(randomColumn(random));
    }

    private static String randomColumn(Random random) {
        return "c" + random.nextInt(COLUMN_COUNT);
    }

    private static String randomTable(Random random) {
        return table(random.nextInt(TABLE_COUNT));
    }

    private static String table(int index) {
        return "table" + index;
    }

    private static Integer randomValue(Random random) {
        int val = random.nextInt(MAX_VALUE-MIN_VALUE) + MIN_VALUE;
        if (val == MAX_VALUE) {
            return null;
        } else {
            return val;
        }
    }

    private void insertRows(Random random, int tableIndex) {
        int row_count = random.nextInt(MAX_ROW_COUNT);
        for (int j=0; j<row_count; j++) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(table(tableIndex));
            sb.append(" VALUES (");
            sb.append(j);
            for (int k=0; k<COLUMN_COUNT; k++) {
                sb.append(",");
                sb.append(randomValue(random));
            }
            sb.append(")");
            sql(sb.toString());
        }
    }

    private static String createIndexSql(Random random, String indexName) {
        List<String> columns = new ArrayList<>();
        columns.add("main");
        for (int i=0; i<COLUMN_COUNT; i++) {
            columns.add("c" + i);
        }
        int columnsInIndex = random.nextInt(COLUMN_COUNT);
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("INDEX ");
        sb.append(indexName);
        sb.append( " ON ");
        sb.append(randomTable(random));
        sb.append("(");
        sb.append(columns.remove(random.nextInt(COLUMN_COUNT)));
        while (!columns.isEmpty()) {
            if (random.nextInt(4) == 0) {
                break;
            }
            sb.append(", ");
            sb.append(columns.remove(random.nextInt(columns.size())));
        }
        sb.append(")");
        return sb.toString();
    }

    @Before
    public void setup() {
        // RandomRule is used to generate parameters, so that we have different DDL sets of tests
        Random random = new Random(testSeed);
        for (int i=0; i<TABLE_COUNT; i++) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("CREATE TABLE ");
            stringBuilder.append(table(i));
            stringBuilder.append(" (main INT PRIMARY KEY");
            for (int j=0; j<COLUMN_COUNT; j++) {
                stringBuilder.append(", c");
                stringBuilder.append(j);
                stringBuilder.append(" INT");
            }
            stringBuilder.append(")");
            sql(stringBuilder.toString());
            insertRows(random, i);
        }

        int indexCount = random.nextInt(MAX_INDEX_COUNT);
        for (int k=0; k<indexCount; k++) {
            sql(createIndexSql(random, "index" + k));
        }
        // TODO create random groups & group indexes
    }

    @After
    public void teardown() {
        if (hitPostgres) {
            // our teardown method won't work against postgres because it uses drop schema magic,
            // just drop the tables individually.
            for (int i = 0; i < TABLE_COUNT; i++) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("DROP TABLE ");
                stringBuilder.append(table(i));
                sql(stringBuilder.toString());
            }
        }
    }

    @Test
    public void Test() {
        Random random = new Random(testSeed);
        for (int i=0; i<QUERY_COUNT; i++) {
            LOG.debug("Query #{}", i);
            boolean useExists  = random.nextBoolean();
            int limitOutside = random.nextInt(MAX_OUTER_LIMIT * 10);
            TableAliasGenerator tag = new TableAliasGenerator(random);
            if (useExists) {
                testOneQueryExists(buildQuery(random, useExists, true, tag), buildQuery(random, useExists, false, tag),
                        limitOutside);
            } else {
                testOneQueryIn(buildQuery(random, useExists, true, tag), buildQuery(random, useExists, false, tag),
                        limitOutside);
            }
        }
    }

    private void testOneQueryExists(String query1, String query2, int limitOutside) {
        boolean negative = randomRule.getRandom().nextBoolean();
        String existsClause = negative ? "NOT EXISTS" : "EXISTS";
        boolean query1IsJustATable = query1.startsWith("table");
        LOG.debug("Outer: {}", query1);
        LOG.debug("Inner: {}", query2);
        List<List<?>> results = sql(query1IsJustATable ? "SELECT main FROM " + query1 : query1);
        List<Integer> expected = new ArrayList<>();
        for (List<?> outerRow : results) {
            List<List<?>> innerResults = sql(String.format(query2, outerRow.get(0)));
            if (negative == (innerResults.size() == 0)) {
                expected.add((Integer) outerRow.get(0));
            }
        }
        String q1 = query1IsJustATable ? query1 : "(" + query1 + ")";
        String finalQuery = "SELECT main FROM " + q1 + " AS T1 WHERE " + existsClause +
                " (" + String.format(query2, "T1.main") + ")" + finalQueryLimit(limitOutside);
        LOG.debug("Final: {}", finalQuery);
        List<List<?>> sqlResults = sql(finalQuery);
        List<Integer> actual = new ArrayList<>();
        for (List<?> actualRow : sqlResults) {
            assertEquals("Expected 1 column" + actualRow, 1, actualRow.size());
            actual.add((Integer) actualRow.get(0));
        }
        Collections.sort(expected, new NullableIntegerComparator());
        Collections.sort(actual, new NullableIntegerComparator());
        expected = applyLimit(expected, limitOutside);
        assertEqualLists("Results different for " + finalQuery, expected, actual);
    }

    private void testOneQueryIn(String query1, String query2, int limitOutside) {
        boolean useIn = randomRule.getRandom().nextBoolean();
        String inClause = useIn ? "IN" : "NOT IN";
        LOG.debug("Outer: {}", query1);
        LOG.debug("Inner: {}", query2);
        boolean query1IsJustATable = query1.startsWith("table");
        List<List<?>> results1 = sql(query1IsJustATable ? "SELECT main FROM " + query1 : query1);
        List<List<?>> results2 = sql(query2);
        boolean insideIsAllNull = results2.size() > 0;
        for (List<?> row : results2) {
            if (row.get(0) != null) {
                insideIsAllNull = false;
                break;
            }
        }
        // if everything in the right hand side is null, it doesn't matter what's on the left hand side
        // nothing will be expected, even for NOT IN
        List<Integer> expected = new ArrayList<>();
        if (!insideIsAllNull) {
            for (List<?> row : results1) {
                // null not in t never gets returned
                if (row.get(0) == null) {
                    continue;
                }
                boolean rowIsInResults2 = false;
                for (List<?> row2 : results2) {
                    if (nullableEquals(row.get(0), row2.get(0))) {
                        rowIsInResults2 = true;
                        break;
                    }
                }
                if (useIn == rowIsInResults2) {
                    expected.add((Integer) row.get(0));
                }
            }
        }
        String q1 = query1IsJustATable ? query1 : "(" + query1 + ")";
        String finalQuery = "SELECT main FROM " + q1 + " AS T1 WHERE main " + inClause + " (" + query2 + ")" +
                finalQueryLimit(limitOutside);
        LOG.debug("Final: {}", finalQuery);
        List<List<?>> sqlResults = sql(finalQuery);
        List<Object> actual = new ArrayList<>();
        for (List<?> actualRow : sqlResults) {
            assertEquals("Expected 1 column" + actualRow, 1, actualRow.size());
            actual.add(actualRow.get(0));
        }
        expected = applyLimit(expected, limitOutside);
        assertEqualLists("Results different for " + finalQuery, expected, actual);
    }

    private List<Integer> applyLimit(List<Integer> expected, int limitOutside) {
        if (limitOutside < MAX_OUTER_LIMIT) {
            Collections.sort(expected, new NullableIntegerComparator());
            if (limitOutside+1 < expected.size()) {
                return expected.subList(0, limitOutside + 1);
            }
        }
        return expected;
    }

    private String finalQueryLimit(int limitOutside) {
        if (limitOutside < 10) {
            return " ORDER BY T1.main LIMIT " + (limitOutside + 1);
        } else {
            return "";
        }
    }

    /**
     * Object comparison in the same way that SQL does it, i.e. null != null
     */
    private boolean nullableEquals(Object o1, Object o2) {
        if (o1 == null || o2 == null) {
            return false;
        }
        return o1.equals(o2);
    }

    private class NullableIntegerComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            if (Objects.equals(o1, o2)) {
                return 0;
            }
            if (o1 == null) {
                return Integer.MIN_VALUE;
            }
            if (o2 == null) {
                return Integer.MAX_VALUE;
            }
            return o1-o2;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof NullableIntegerComparator;
        }
    }


    private class TableAliasGenerator {

        private Random random;
        private int count = 0;

        public TableAliasGenerator(Random random) {
            this.random = random;
        }

        int createNew() {
            return count++;
        }

        String toString(int index) {
            return "ta" + index;
        }

        int randomAbove(int min) {
            return random.nextInt(count-min) + min;
        }

        int randomAbove(int min, int excluded) {
            int secondTable = randomAbove(min);
            if (secondTable == excluded) {
                secondTable++;
                if (secondTable == count) {
                    secondTable = min;
                }
            }
            return secondTable;
        }
    }
}
