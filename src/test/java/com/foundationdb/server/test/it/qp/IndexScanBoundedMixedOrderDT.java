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
import java.util.Objects;
import java.util.Random;

import com.foundationdb.junit.SelectedParameterizedRunner;

import com.foundationdb.util.Strings;
import org.junit.ComparisonFailure;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(SelectedParameterizedRunner.class)
public class IndexScanBoundedMixedOrderDT extends IndexScanUnboundedMixedOrderDT {

    static final Integer MAX_VALUE = 100;
    static final Integer MIN_VALUE = 0;
    static final Integer TOTAL_PERMS = 64;

    private Integer[] lower_bounds;
    private Integer[] upper_bounds;
    private Boolean[] lower_inclusive;
    private Boolean[] upper_inclusive;

    private boolean[] skipped = new boolean[]{false, false, false, false};


    public IndexScanBoundedMixedOrderDT(List<OrderByOptions> orderings, Integer[] loBounds, Integer[] hiBounds,
                                        Boolean[] loInclusive, Boolean[] hiInclusive) {
        super(null /*name*/, orderings);
        this.lower_bounds = loBounds;
        this.upper_bounds = hiBounds;
        this.lower_inclusive = loInclusive;
        this.upper_inclusive = hiInclusive;
    }

    @Override
    protected void compare(List<List<Integer>> expectedResults, List<List<?>> results) {
        if(expectedResults.size() != results.size()) {
            throw new ComparisonFailure("result size", Strings.join(expectedResults), Strings.join(results));
        }
        for (int i = 0; i < expectedResults.size(); i++) {
            List resultRow = results.get(i);
            if(expectedResults.get(i).size() != resultRow.size()) {
                throw new ComparisonFailure("row size", Strings.join(expectedResults), Strings.join(results));
            }
            for (int j = 0; j < resultRow.size(); j++) {
                if (orderings.get(j) != OrderByOptions.NONE) {
                    if(!Objects.equals(expectedResults.get(i).get(j), resultRow.get(j))) {
                        throw new ComparisonFailure("order of row "+i+" col "+j, Strings.join(expectedResults), Strings.join(results));
                    }
                }
                if (!skipped[j] && (!withinBounds((Integer)resultRow.get(j), lower_bounds[j], lower_inclusive[j], true) ||
                        !withinBounds((Integer)resultRow.get(j), upper_bounds[j], upper_inclusive[j], false))) {
                    throw new ComparisonFailure("bounds of row "+i+" col "+j, Strings.join(expectedResults), Strings.join(results));
                }
            }
        }
    }

    @Override
    protected String createQuery() {
        String query = "SELECT " + Strings.join(COLUMNS, ",") + " FROM " + TABLE_NAME;
        String conditions = "";
        boolean hasConditions = false;
        for (int i = 0; i < lower_bounds.length; i++) {
            if (R.nextBoolean()) {
                String lower_bound, upper_bound;
                if (lower_bounds[i] == null) lower_bound = "null";
                else lower_bound = Integer.toString(lower_bounds[i]);
                if (upper_bounds[i] == null) upper_bound = "null";
                else upper_bound = Integer.toString(upper_bounds[i]);

                if (lower_inclusive[i]) {
                    conditions = conditions + COLUMNS.get(i) + " >= " + lower_bound + " AND ";
                } else {
                    conditions = conditions + COLUMNS.get(i) + " > " + lower_bound + " AND ";
                }
                if (upper_inclusive[i]) {
                    conditions = conditions + COLUMNS.get(i) + " <= " + upper_bound + " AND ";
                } else {
                    conditions = conditions + COLUMNS.get(i) + " < " + upper_bound + " AND ";
                }
                hasConditions = true;
            } else {
                skipped[i] = true;
            }
        }

        if (hasConditions) query = query +  " WHERE " + conditions.substring(0, conditions.length() - 5);

        query = query + " ORDER BY ";
        for (int i = 0; i < orderings.size(); i++) {
            String oStr = orderings.get(i).getOrderingString();
            if (oStr != null) {
                query = query + COLUMNS.get(i) + " " + oStr + ", ";
            }
        }
        return query.substring(0, query.length() - 2) + ";";
    }

    @Override
    protected List<List<Integer>> expectedRows() {
        List<List<Integer>> newRows = super.expectedRows();
        return filterRows(newRows);
    }

    protected List<List<Integer>> filterRows(List<List<Integer>> newRows) {
        List<List<Integer>> expected = new ArrayList<>();
        for(List<Integer> row : newRows) {
            boolean add = true;
            for (int j = 0; j < lower_bounds.length; j++) {
                if (skipped[j]) continue;
                if (!withinBounds(row.get(j), lower_bounds[j], lower_inclusive[j], true)) {
                    add = false;
                }
                if (!withinBounds(row.get(j), upper_bounds[j], upper_inclusive[j], false)) {
                    add = false;
                }
            }
            if (add) expected.add(row);
        }
        return expected;
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
    public static List<Object[]> params() throws Exception {
        Collection<List<OrderByOptions>> orderByPerms = IndexScanUnboundedMixedOrderDT.orderByPermutations();
        List<Object[]> params = new ArrayList<>();
        for(List<OrderByOptions> ordering : orderByPerms) {
            boolean nonEmpty = false;
            for(OrderByOptions o : ordering) {
                if(o.getOrderingString() != null) {
                    nonEmpty = true;
                    break;
                }
            }
            if(nonEmpty) {
                Integer[] loBound = getLowerBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, R);
                Integer[] hiBound = getUpperBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, loBound, R);
                Boolean[] loInclusive = getInclusive(TOTAL_COLS, R);
                Boolean[] hiInclusive = getInclusive(TOTAL_COLS, R);
                Object[] param = new Object[]{ ordering, loBound, hiBound, loInclusive, hiInclusive };
                params.add(param);
            }
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
