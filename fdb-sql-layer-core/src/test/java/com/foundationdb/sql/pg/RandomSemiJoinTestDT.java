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
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.util.RandomRule;
import com.foundationdb.util.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import sun.jvm.hotspot.utilities.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Randomly generate 2 queries, then
 * run Query1 & Query2 and store results
 * do
 *   SELECT id FROM (Query1) WHERE id IN (Query2)
 * Manually compute results from stored results,
 * and compare with whole query.
 */
@RunWith(SelectedParameterizedRunner.class)
public class RandomSemiJoinTestDT extends PostgresServerITBase {

    private static final int TEST_COUNT = 30;
    private static final int TABLE_COUNT = 3;
    private static final int COLUMN_COUNT = 10;
    private static final int MAX_ROW_COUNT = 100;

    @ClassRule
    public static final RandomRule randomRule = new RandomRule();
    private String query1;
    private String query2;

    @Parameterized.Parameters(name="SELECT main FROM ({0}) AS T1 WHERE id IN ({1})")
    public static List<Object[]> params() throws Exception {
        Random random = randomRule.reset();
        List<Object[]> params = new ArrayList<>(TEST_COUNT);
        for (int i=0; i<TEST_COUNT; i++) {
            params.add(new Object[]{buildQuery(random, true), buildQuery(random, false)});
        }
        return params;
    }

    public RandomSemiJoinTestDT(String query1, String query2) {
        this.query1 = query1;
        this.query2 = query2;
    }

    private static String buildQuery(Random random, boolean firstQuery) {
        // TODO for real
        if (firstQuery && random.nextInt(20) == 0) {
            return randomTable(random);
        }
        StringBuilder stringBuilder = new StringBuilder();
        // TODO pick which table of the joins to grab main from ?
        stringBuilder.append("SELECT TAB1.");
        stringBuilder.append(firstQuery ? "main" : randomColumn(random));
        stringBuilder.append(" FROM ");
        stringBuilder.append(randomTable(random));
        stringBuilder.append(" AS TAB1");
        switch (random.nextInt(2)) {
            case 0:
                // Just the FROM
                break;
            case 1:
                // INNER JOIN
                addInnerJoin(stringBuilder, random);
                break;
            default:
                throw new IllegalStateException("not enough casses for random values");
        }

        return stringBuilder.toString();
    }

    private static void addInnerJoin(StringBuilder sb, Random random) {
        // TODO incrementing counter
        sb.append(" INNER JOIN ");
        sb.append(randomTable(random));
        sb.append(" AS TAB2 ON ");
        // no cross joins right now
        int conditionCount = random.nextInt(3);
        for (int i=0; i<conditionCount+1; i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            boolean first1 = random.nextBoolean();
            sb.append(first1 ? "TAB1." : "TAB2.");
            sb.append(randomColumn(random));
            sb.append(" = "); // TODO random comparison
            sb.append(first1 ? "TAB2." : "TAB1.");
            sb.append(randomColumn(random));
        }
    }

    private static String randomTable(Random random) {
        return table(random.nextInt(TABLE_COUNT));
    }

    private static String randomColumn(Random random) {
        return "column" + random.nextInt(COLUMN_COUNT);
    }

    private static String table(int index) {
        return "table" + index;
    }

    @Before
    public void setup() {
        // TODO get seed for method from test info, so that these are different, but still deterministic
        Random random = randomRule.getRandom();
        for (int i=0; i<TABLE_COUNT; i++) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("CREATE TABLE ");
            stringBuilder.append(table(i));
            stringBuilder.append(" (main INT PRIMARY KEY");
            for (int j=0; j<COLUMN_COUNT; j++) {
                stringBuilder.append(", column");
                stringBuilder.append(j);
                stringBuilder.append(" INT");
            }
            stringBuilder.append(")");
            sql(stringBuilder.toString());
            fillRowData(random, i);
        }
        // TODO create random indexes
    }

    @After
    public void teardown() {
        for (int i=0; i<TABLE_COUNT; i++) {
            sql("DROP TABLE " + table(i));
        }
    }

    public void fillRowData(Random random, int tableIndex) {
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
                sb.append(random.nextInt());
            }
            sb.append(")");
            sql(sb.toString());
        }
    }


    @Test
    public void Test() {
        List<List<?>> results1 = sql(query1.startsWith("table") ? "SELECT main FROM " + query1 : query1);
        List<List<?>> results2 = sql(query2);
        List<Object> expected = new ArrayList<>();
        for (List<?> row : results1) {
            for (List<?> row2 : results2) {
                // TODO what about null
                if (row.get(0).equals(row2.get(0))) {
                    expected.add((Object)row.get(0));
                }
            }
        }
        List<List<?>> sqlResults = sql("SELECT main FROM (" + query1 + ") AS T1 WHERE main IN (" + query2 + ")");
        List<Object> actual = new ArrayList<>();
        for (List<?> actualRow : sqlResults) {
            assertEquals("Expected 1 column" + actualRow, 1, actualRow.size());
            actual.add(actualRow.get(0));
        }
        assertEqualLists("Checking lists", expected, actual);
    }
}
