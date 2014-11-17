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
import org.junit.rules.ErrorCollector;
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
//@Ignore("Waiting until this passes most of the time")
@RunWith(SelectedParameterizedRunner.class)
public class RandomSemiJoinTestDT extends PostgresServerITBase {

    private static final Logger LOG = LoggerFactory.getLogger(RandomSemiJoinTestDT.class);

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

    @ClassRule
    public static final RandomRule randomRule = new RandomRule();
    @Rule
    public final RandomRule testRandom = randomRule;
    @Rule
    public final ErrorCollector collector = new ErrorCollector();

    /**
     * The seed used for individual parameterized tests, so that they can have different DDL & DML
     */
    private Long testSeed;

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

    private static int randomValue(Random random) {
        return random.nextInt(MAX_VALUE-MIN_VALUE) + MIN_VALUE;
    }

    private static String buildQuery(Random random, boolean useExists, boolean firstQuery) {
        // TODO for real
        if (firstQuery && random.nextInt(20) == 0) {
            return randomTable(random);
        }
        StringBuilder stringBuilder = new StringBuilder();
        // TODO pick which table of the joins to grab main from ?
        stringBuilder.append("SELECT ta0.");
        stringBuilder.append(firstQuery ? "main" : randomColumn(random));
        stringBuilder.append(" FROM ");
        stringBuilder.append(randomTable(random));
        stringBuilder.append(" AS ta0");
        int tableAliasCount = 1;
        switch (random.nextInt(4)) {
            case 0:
                // Just the FROM
                break;
            case 1:
                tableAliasCount++;
                addJoin("INNER", stringBuilder, random, tableAliasCount);
                break;
            case 2:
                tableAliasCount++;
                addJoin("LEFT OUTER", stringBuilder, random, tableAliasCount);
                break;
            case 3:
                tableAliasCount++;
                addJoin("RIGHT OUTER", stringBuilder, random, tableAliasCount);
                break;
            default:
                throw new IllegalStateException("not enough cases for random values");
        }
        generateWhereClause(stringBuilder, random, tableAliasCount, !firstQuery && useExists);
        return stringBuilder.toString();
    }

    private static void generateWhereClause(StringBuilder stringBuilder, Random random,
                                            int tableAliasCount, boolean forceMainEqualsClause) {
        if (!forceMainEqualsClause && random.nextInt(5) == 0) {
            return;
        }
        stringBuilder.append(" WHERE ");
        randomCondition(stringBuilder, random, tableAliasCount, WHERE_CONSTANT_LIKELYHOOD, forceMainEqualsClause);
        for (int i=0; i<MAX_CONDITION_COUNT; i++) {
            if (random.nextInt(5) == 0) {
                break;
            }
            stringBuilder.append(random.nextBoolean() ? " AND " : " OR ");
            randomCondition(stringBuilder, random, tableAliasCount, WHERE_CONSTANT_LIKELYHOOD, false);
        }
    }

    private void fillRowData(Random random, int tableIndex) {
        int row_count = random.nextInt(MAX_ROW_COUNT);
        for (int j=0; j<row_count; j++) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ");
            sb.append(table(tableIndex));
            sb.append(" VALUES (");
            sb.append(j);
            for (int k=0; k<COLUMN_COUNT; k++) {
                sb.append(",");
                // TODO sometimes null
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

    private static void addJoin(String type, StringBuilder sb, Random random, int tableAliasCount) {
        // TODO incrementing counter
        sb.append(" ");
        sb.append(type);
        sb.append(" JOIN ");
        sb.append(randomTable(random));
        sb.append(" AS ta");
        sb.append(tableAliasCount-1);
        sb.append(" ON ");
        // no cross joins right now
        int conditionCount = random.nextInt(3);
        for (int i=0; i<conditionCount+1; i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            randomCondition(sb, random, tableAliasCount, JOIN_CONSTANT_LIKELYHOOD, false);
        }
    }

    private static void randomCondition(StringBuilder sb, Random random, int tableAliasCount, int constantBias,
                                        boolean forceMainEqualsClause) {
        int firstTable = random.nextInt(tableAliasCount);
        int secondTable = random.nextInt(tableAliasCount);
        if (secondTable == firstTable) {
            secondTable = (firstTable + 1) % tableAliasCount;
        }
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
                aliasedSource(sb, random, firstTable);
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
                aliasedSource(sb, random, secondTable);
            } else {
                sb.append("%s");
            }
        }
    }

    private static void aliasedSource(StringBuilder sb, Random random, int firstTable) {
        sb.append("ta");
        sb.append(firstTable);
        sb.append(".");
        sb.append(randomColumn(random));
    }

    private static String randomTable(Random random) {
        return table(random.nextInt(TABLE_COUNT));
    }

    private static String randomColumn(Random random) {
        return "c" + random.nextInt(COLUMN_COUNT);
    }

    private static String table(int index) {
        return "table" + index;
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
            fillRowData(random, i);
        }

        int indexCount = random.nextInt(MAX_INDEX_COUNT);
        for (int k=0; k<indexCount; k++) {
            sql(createIndexSql(random, "index" + k));
        }
        // TODO create random groups & group indexes
    }

    @After
    public void teardown() {
        for (int i=0; i<TABLE_COUNT; i++) {
            sql("DROP TABLE " + table(i));
        }
    }


    @Test
    public void Test() {
        Random random = new Random(testSeed);
        for (int i=0; i<QUERY_COUNT; i++) {
            boolean useExists  = random.nextBoolean();
            if (useExists) {
                testOneQueryExists(buildQuery(random, useExists, true), buildQuery(random, useExists, false));
            } else {
                testOneQueryIn(buildQuery(random, useExists, true), buildQuery(random, useExists, false));
            }
        }
    }

    private void testOneQueryExists(String query1, String query2) {
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
                " (" + String.format(query2, "T1.main") + ")";
        LOG.debug("Final: {}", finalQuery);
        List<List<?>> sqlResults = sql(finalQuery);
        List<Integer> actual = new ArrayList<>();
        for (List<?> actualRow : sqlResults) {
            assertEquals("Expected 1 column" + actualRow, 1, actualRow.size());
            actual.add((Integer) actualRow.get(0));
        }
        Collections.sort(expected, new NullableIntegerComparator());
        Collections.sort(actual, new NullableIntegerComparator());
        assertEqualLists("Results different for " + finalQuery, expected, actual);
    }

    private void testOneQueryIn(String query1, String query2) {
        boolean useIn = randomRule.getRandom().nextBoolean();
        String inClause = useIn ? "IN" : "NOT IN";
        LOG.debug("Outer: {}", query1);
        LOG.debug("Inner: {}", query2);
        boolean query1IsJustATable = query1.startsWith("table");
        List<List<?>> results1 = sql(query1IsJustATable ? "SELECT main FROM " + query1 : query1);
        List<List<?>> results2 = sql(query2);
        List<Integer> expected = new ArrayList<>();
        for (List<?> row : results1) {
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
        String q1 = query1IsJustATable ? query1 : "(" + query1 + ")";
        String finalQuery = "SELECT main FROM " + q1 + " AS T1 WHERE main " + inClause + " (" + query2 + ")";
        LOG.debug("Final: {}", finalQuery);
        List<List<?>> sqlResults = sql(finalQuery);
        List<Object> actual = new ArrayList<>();
        for (List<?> actualRow : sqlResults) {
            assertEquals("Expected 1 column" + actualRow, 1, actualRow.size());
            actual.add(actualRow.get(0));
        }
        assertEqualLists("Results different for " + finalQuery, expected, actual);
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
}
