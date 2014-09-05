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

package com.foundationdb.server.test.it.qp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import com.foundationdb.junit.SelectedParameterizedRunner;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;


@RunWith(SelectedParameterizedRunner.class)
public class IndexScanBoundedMixedOrderDT extends IndexScanUnboundedMixedOrderDT {

    static final Integer MAX_VALUE = 100;
    static final Integer MIN_VALUE = 0;

    private static final String[] COLUMNS = {"t0", "t1", "t2", "t3"};

    private Integer[] lower_bounds;
    private Integer[] upper_bounds;
    private Boolean[] lower_inclusive;
    private Boolean[] upper_inclusive;

    private boolean[] skipped = new boolean[]{false, false, false, false};


    public IndexScanBoundedMixedOrderDT(String[] orderings, Integer[] loBounds, Integer[] hiBounds,
            Boolean[] loInclusive, Boolean[] hiInclusive) {
        super(orderings[0], orderings[1], orderings[2], orderings[3]);
        this.lower_bounds = loBounds;
        this.upper_bounds = hiBounds;
        this.lower_inclusive = loInclusive;
        this.upper_inclusive = hiInclusive;
    }

    @Override
    void compare(Integer[][] expectedResults,
            List<List<?>> results) {
        assertEquals("Failed with seed " + Long.toString(seed) + " and query: " + query, expectedResults.length, results.size());
        for (int i = 0; i < expectedResults.length; i++) {
            List resultRow = results.get(i);
            assertEquals("Failed with seed " + Long.toString(seed) + " and query: " + query, expectedResults[i].length, resultRow.size());
            for (int j = 1; j < resultRow.size(); j++) {
                if (!orderings[j-1].equals("")) {
                    assertEquals("Failed with seed " + Long.toString(seed) + " and query: " + query, expectedResults[i][j], resultRow.get(j));
                }
                if (!skipped[j-1] && (!withinBounds((Integer)resultRow.get(j), lower_bounds[j-1], lower_inclusive[j-1], true) ||
                        !withinBounds((Integer)resultRow.get(j), upper_bounds[j-1], upper_inclusive[j-1], false))) {
                    assertEquals("Failed with seed " + Long.toString(seed) + " and query: " + query, expectedResults[i][j], resultRow.get(j));
                }
            }
        }
    }

    @Override
    String createQuery() {
        String query = QUERY_BASE;
        String conditions = "";
        boolean hasConditions = false;
        for (int i = 0; i < lower_bounds.length; i++) {
            if (r.nextBoolean()) {
                String lower_bound, upper_bound;
                if (lower_bounds[i] == null) lower_bound = "null";
                else lower_bound = Integer.toString(lower_bounds[i]);
                if (upper_bounds[i] == null) upper_bound = "null";
                else upper_bound = Integer.toString(upper_bounds[i]);

                if (lower_inclusive[i]) {
                    conditions = conditions + COLUMNS[i] + " >= " + lower_bound + " AND ";
                } else {
                    conditions = conditions + COLUMNS[i] + " > " + lower_bound + " AND ";
                }
                if (upper_inclusive[i]) {
                    conditions = conditions + COLUMNS[i] + " <= " + upper_bound + " AND ";
                } else {
                    conditions = conditions + COLUMNS[i] + " < " + upper_bound + " AND ";
                }
                hasConditions = true;
            } else {
                skipped[i] = true;
            }
        }

        if (hasConditions) query = query +  " WHERE " + conditions.substring(0, conditions.length() - 5);

        query = query + " ORDER BY ";
        for (int i = 0; i < orderings.length; i++) {
            if (!orderings[i].equals("")) {
                query = query + COLUMNS[i] + " " + orderings[i] + ", ";
            }
        }
        return query.substring(0, query.length() - 2) + ";";
    }

    @Override
    Integer[][] expectedRows() {
        Integer[][] newRows = super.expectedRows();
        return filterRows(newRows);
    }

    Integer[][] filterRows(Integer[][] newRows) {
        ArrayList<Integer[]> expected = new ArrayList<Integer[]>();
        for (int i = 0; i < newRows.length; i++) {
            boolean add = true;
            for (int j = 0; j < lower_bounds.length; j++) {
                if (skipped[j]) continue;
                if (!withinBounds(newRows[i][j+1], lower_bounds[j], lower_inclusive[j], true)) {
                    add = false;
                }
                if (!withinBounds(newRows[i][j+1], upper_bounds[j], upper_inclusive[j], false)) {
                    add = false;
                }
            }
            if (add) expected.add(newRows[i]);
        }
        Integer[][] expectedArray = new Integer[expected.size()][TOTAL_COLS + 1];
        for (int i = 0; i < expected.size(); i++) {
            expectedArray[i] = expected.get(i);
        }
        return expectedArray;
    }

    static Boolean withinBounds(Integer value, Integer bound, Boolean inclusive, Boolean lower) {
        if (value == null || bound == null) return false;
        if (value < bound) {
            return !lower;
        }
        if (value > bound) {
            return lower;
        }
        return inclusive;
    }

    @Parameters
    public static Iterable<Object[][]> params() throws Exception {
        Collection<Object[][]> params = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String[] ordering = getPermutation(OPTIONS, r.nextInt(TOTAL_PERMS));
            Integer[] loBound = getLowerBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, r);
            Integer[] hiBound = getUpperBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, loBound, r);
            Boolean[] loInclusive = getInclusive(TOTAL_COLS, r);
            Boolean[] hiInclusive = getInclusive(TOTAL_COLS, r);
            Object[][] param = new Object[][]{ordering, loBound, hiBound, loInclusive, hiInclusive};
            params.add(param);
        }
        return params;
    }

    public static Integer[] getLowerBounds(int min, int max, int cols, Random r) {
        Integer[] bounds = new Integer[cols];
        for (int i = 0; i < cols; i++) {
            bounds[i] = r.nextInt(max - min) + min;
            if (r.nextInt(10) == 1) bounds[i] = null;
        }
        return bounds;
    }

    public static Integer[] getUpperBounds(int min, int max, int cols, Integer[] lowerBounds, Random r) {
        Integer[] bounds = new Integer[cols];
        for (int i = 0; i < cols; i++) {
            if (lowerBounds[i] == null) {
                bounds[i] = r.nextInt(max);
            } else {
                bounds[i] = r.nextInt(max - lowerBounds[i] + 1) + lowerBounds[i] - 1;
            }
            if (r.nextInt(10) == 1) bounds[i] = null;
        }
        return bounds;
    }

    public static Boolean[] getInclusive(int cols, Random r) {
        Boolean[] bounds = new Boolean[cols];
        for (int i = 0; i < cols; i++) {
            bounds[i] = r.nextBoolean();
        }
        return bounds;
    }
}
