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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.util.Strings;

import org.junit.ComparisonFailure;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;


@RunWith(SelectedParameterizedRunner.class)
public class IndexScanBoundedMixedOrderDT extends IndexScanUnboundedMixedOrderDT {

    private List<Integer> loBounds;
    private List<Integer> hiBounds;
    private List<Boolean> loInclusive;
    private List<Boolean> hiInclusive;

    private List<Boolean> skipped = Arrays.asList(false, false, false, false);


    public IndexScanBoundedMixedOrderDT(String name, List<OrderByOptions> orderings, List<Integer> loBounds, List<Integer> hiBounds,
                                        List<Boolean> loInclusive, List<Boolean> hiInclusive) {
        super(name, orderings);
        this.loBounds = loBounds;
        this.hiBounds = hiBounds;
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void compare(List<List<Integer>> expectedResults, List<List<?>> results) {
        int eSize = expectedResults.size();
        int aSize = results.size();
        boolean match = true;
        boolean bounds = true;
        for(int i = 0; match && bounds && i < Math.min(eSize, aSize); ++i) {
            match = rowComparator.compare(expectedResults.get(i), (List<Integer>)results.get(i)) == 0;
            bounds = withinBounds(results.get(i), loBounds, loInclusive, hiBounds, hiInclusive, skipped);
        }
        if(!match || !bounds || (eSize != aSize)) {
            throw new ComparisonFailure("row mismatch", Strings.join(expectedResults), Strings.join(results));
        }
    }

    @Override
    protected String createQuery() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT ");
        for(int i = 0; i < TOTAL_COLS; ++i) {
            if(i > 0) {
                queryBuilder.append(", ");
            }
            queryBuilder.append(COLUMNS.get(i));
        }
        queryBuilder.append(" FROM ");
        queryBuilder.append(TABLE_NAME);

        queryBuilder.append(createConditions());
        queryBuilder.append(createOrderings());
        return queryBuilder.toString();
    }

    private StringBuilder createConditions() {
        StringBuilder conditionsBuilder = new StringBuilder();
        boolean hasConditions = false;
        for (int i = 0; i < loBounds.size(); i++) {
            if (R.nextBoolean()) {
                if (hasConditions) {
                    conditionsBuilder.append(" AND ");
                } else {
                    conditionsBuilder.append(" WHERE ");
                    hasConditions = true;
                }

                if (loBounds.get(i) == null || hiBounds.get(i) == null) {
                    conditionsBuilder.append(COLUMNS.get(i) + " IS NULL");
                    continue;
                }                

                String lower_bound, upper_bound;
                lower_bound = Integer.toString(loBounds.get(i));
                upper_bound = Integer.toString(hiBounds.get(i));

                if (loInclusive.get(i)) {
                    conditionsBuilder.append(COLUMNS.get(i) + " >= " + lower_bound + " AND ");
                } else {
                    conditionsBuilder.append(COLUMNS.get(i) + " > " + lower_bound + " AND ");
                }
                if (hiInclusive.get(i)) {
                    conditionsBuilder.append(COLUMNS.get(i) + " <= " + upper_bound);
                } else {
                    conditionsBuilder.append(COLUMNS.get(i) + " < " + upper_bound);
                }
            } else {
                skipped.set(i, true);
            }
        }
        return conditionsBuilder;
    }

    private StringBuilder createOrderings() {
        StringBuilder orderingsBuilder = new StringBuilder();
        orderingsBuilder.append(" ORDER BY ");
        boolean firstOrdering = true;
        for (int i = 0; i < orderings.size(); i++) {
            String oStr = orderings.get(i).getOrderingString();
            if (oStr != null && firstOrdering) {
                orderingsBuilder.append(COLUMNS.get(i) + " " + oStr);
                firstOrdering = false;
            }
            else if (oStr != null) {
                orderingsBuilder.append(", " + COLUMNS.get(i) + " " + oStr);
            }
        }
        return orderingsBuilder;
    }

    @Override
    protected List<List<Integer>> expectedRows() {
        List<List<Integer>> newRows = super.expectedRows();
        return filterRows(newRows);
    }

    protected List<List<Integer>> filterRows(List<List<Integer>> newRows) {
        List<List<Integer>> expected = new ArrayList<>();
        for(List<Integer> row : newRows) {
            if (withinBounds(row, loBounds, loInclusive, hiBounds, hiInclusive, skipped)) {
                expected.add(row);
            }
        }
        return expected;
    }

    protected static Boolean withinBounds(List<?> values, List<Integer> loBounds, List<Boolean> loInclusive,
            List<Integer> hiBounds, List<Boolean> hiInclusive, List<Boolean> skipped) {
        for (int i = 0; i < values.size(); i++) {
            if (!skipped.get(i) && !(withinBounds((Integer)values.get(i), loBounds.get(i), loInclusive.get(i), 
                    hiBounds.get(i), hiInclusive.get(i)))) {
                return false;
            }
        }
        return true;
    }

    protected static Boolean withinBounds(Integer value, Integer loBound, Boolean loInclusive, Integer hiBound,
            Boolean hiInclusive) {
        if (value == null && (loBound == null || hiBound == null)) {
            return true;
        }
        if (value == null || loBound == null || hiBound == null) {
            return false;
        }
        if (value < loBound) {
            return false;
        }
        if (value > hiBound) {
            return false;
        }
        if (value == hiBound) {
            return hiInclusive;
        }
        if (value == loBound) {
            return loInclusive;
        }
        return true;
    }

    @Parameters(name="{0}")
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
                String name = makeTestName(ordering);
                List<Integer> loBounds = getLowerBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, R);
                List<Integer> hiBounds = getUpperBounds(MIN_VALUE, MAX_VALUE, TOTAL_COLS, loBounds, R);
                List<Boolean> loInclusive = getInclusive(TOTAL_COLS, R);
                List<Boolean> hiInclusive = getInclusive(TOTAL_COLS, R);
                Object[] param = new Object[]{ name, ordering, loBounds, hiBounds, loInclusive, hiInclusive };
                params.add(param);
            }
        }
        return params;
    }

    protected static List<Integer> getLowerBounds(int min, int max, int cols, Random r) {
        List<Integer> bounds = new ArrayList<Integer>();
        for (int i = 0; i < cols; i++) {
            if (r.nextInt(10) == 1) {
                bounds.add(null);
            } else {
                bounds.add(r.nextInt(max - min) + min);
            }
        }
        return bounds;
    }

    protected static List<Integer> getUpperBounds(int min, int max, int cols, List<Integer> lowerBounds, Random r) {
        List<Integer> bounds = new ArrayList<Integer>();
        for (int i = 0; i < cols; i++) {
            if (r.nextInt(10) == 1) {
                bounds.add(null);
            } else if (lowerBounds.get(i) == null) {
                bounds.add(r.nextInt(max - min) + min);
            } else {
                bounds.add(r.nextInt(max - lowerBounds.get(i) + 1) + lowerBounds.get(i) - 1);
            }
        }
        return bounds;
    }

    protected static List<Boolean> getInclusive(int cols, Random r) {
        List<Boolean> bounds = new ArrayList<Boolean>();
        for (int i = 0; i < cols; i++) {
            bounds.add(r.nextBoolean());
        }
        return bounds;
    }
}
