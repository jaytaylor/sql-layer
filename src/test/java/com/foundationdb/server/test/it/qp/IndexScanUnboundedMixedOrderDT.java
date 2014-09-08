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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.sql.pg.PostgresServerITBase;

import com.foundationdb.util.Strings;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

@RunWith(SelectedParameterizedRunner.class)
public class IndexScanUnboundedMixedOrderDT extends PostgresServerITBase
{
    protected static final String TABLE_NAME ="t";
    protected static final String INDEX_NAME = "idx";
    protected static final List<String> COLUMNS = Arrays.asList("t0", "t1", "t2", "t3");
    protected static final Integer TOTAL_ROWS = 100;
    protected static final Integer TOTAL_COLS = COLUMNS.size();

    @SuppressWarnings("unchecked")
    static final Comparator ASC_COMPARATOR = new Comparator()
    {
        @Override
        public int compare(Object o1, Object o2) {
            if(o1 == null) {
                return (o2 == null) ? 0 : -1;
            }
            if(o2 == null) {
                return 1;
            }
            return ((Comparable)o1).compareTo(o2);
        }
    };

    static final Comparator DESC_COMPARATOR = new Comparator()
    {
        @Override
        public int compare(Object o1, Object o2) {
            return - ASC_COMPARATOR.compare(o1, o2);
        }
    };


    static enum OrderByOptions {
        NONE,
        ASC,
        DESC
        ;

        @SuppressWarnings("unchecked")
        public <T> Comparator<T> getComparator(Class<T> clazz) {
            switch(this) {
                case NONE: return null;
                case ASC: return ASC_COMPARATOR;
                case DESC: return DESC_COMPARATOR;
                default: throw new IllegalStateException(this.name());
            }
        }

        public String getOrderingString() {
            switch(this) {
                case NONE: return null;
                default: return name();
            }
        }
    }

    static class IndexComparison<T> {
        private final int index;
        private final Comparator<T> comp;

        public IndexComparison(int index, Comparator<T> comp) {
            this.index = index;
            this.comp = comp;
        }
    }

    static class ListComparator<T> implements Comparator<List<T>>
    {
        public final List<IndexComparison<T>> comps;

        public ListComparator(List<IndexComparison<T>> comps) {
            this.comps = comps;
        }

        @Override
        public int compare(List<T> a, List<T> b) {
            assert a.size() == b.size();
            for(IndexComparison<T> c : comps) {
                int i = c.index;
                int r = c.comp.compare(a.get(i), b.get(i));
                if(r != 0) {
                    return r;
                }
            }
            return 0;
        }
    }

    @Rule
    public final TestRule FAILED_WATCHER = new TestWatcher() {
        @Override
        public void failed(Throwable e, Description description) {
            System.err.printf("Query failed with seed %d: %s\n", SEED, IndexScanUnboundedMixedOrderDT.this.query);
        }
    };


    protected static final long SEED = System.currentTimeMillis();
    protected static final Random R = new Random(SEED);

    protected final List<OrderByOptions> orderings;
    protected final List<List<Integer>> DB;
    protected final ListComparator<Integer> rowComparator;
    protected String query;

    public IndexScanUnboundedMixedOrderDT(String name, List<OrderByOptions> orderings) {
        // Reset to ensure DB is consistent
        R.setSeed(SEED);
        this.orderings = orderings;
        this.rowComparator = buildListComparator(orderings);
        this.DB = buildDB(R);
    }

    private static ListComparator<Integer> buildListComparator(List<OrderByOptions> orderings) {
        List<IndexComparison<Integer>> comps = new ArrayList<>();
        for(int i = 0; i < orderings.size(); ++i) {
            Comparator<Integer> comp = orderings.get(i).getComparator(Integer.class);
            if(comp != null) {
                comps.add(new IndexComparison<>(i, comp));
            }
        }
        return new ListComparator<>(comps);
    }

    private static List<List<Integer>> buildDB(Random r) {
        List<List<Integer>> db = new ArrayList<>();
        for (int i = 0; i < TOTAL_ROWS; i++) {
            List<Integer> row = new ArrayList<>();
            for (int j = 0; j < TOTAL_COLS; j++) {
                int next = r.nextInt(110);
                row.add(next > 100 ? null : next);
            }
            db.add(row);
        }
        return db;
    }

    @Before
    public void setup() {
        sql("CREATE TABLE " + TABLE_NAME + "(id SERIAL PRIMARY KEY, t0 INT, t1 INT, t2 INT, t3 INT)");
        sql("CREATE INDEX " + INDEX_NAME + " ON t(t0, t1, t2, t3)");
        StringBuilder sb = new StringBuilder("INSERT INTO " + TABLE_NAME + "(t0,t1,t2,t3) VALUES ");
        for(int i = 0; i < DB.size(); ++i) {
            if(i > 0) {
                sb.append(", ");
            }
            sb.append('(').append(Strings.join(DB.get(i), ",")).append(')');
        }
        sql(sb.toString());
    }

    @Test
    public void testQuery() {
        this.query = createQuery();
        List<List<?>> results = sql(query);
        compare(expectedRows(), results);
    }

    @SuppressWarnings("unchecked")
    protected void compare(List<List<Integer>> expectedResults, List<List<?>> results) {
        int eSize = expectedResults.size();
        int aSize = results.size();
        boolean match = true;
        for(int i = 0; match && i < Math.min(eSize, aSize); ++i) {
            match = rowComparator.compare(expectedResults.get(i), (List<Integer>)results.get(i)) == 0;
        }
        if(!match || (eSize != aSize)) {
            throw new ComparisonFailure("row mismatch", Strings.join(expectedResults), Strings.join(results));
        }
    }

    protected String createQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for(int i = 0; i < TOTAL_COLS; ++i) {
            if(i > 0) {
                sb.append(", ");
            }
            sb.append(COLUMNS.get(i));
        }
        sb.append(" FROM ");
        sb.append(TABLE_NAME);
        boolean first = true;
        for(int i = 0; i < orderings.size(); i++) {
            String oStr = orderings.get(i).getOrderingString();
            if(oStr != null) {
                if(first) {
                    first = false;
                    sb.append(" ORDER BY ");
                } else {
                    sb.append(", ");
                }
                sb.append(COLUMNS.get(i));
                sb.append(" ");
                sb.append(oStr);
            }
        }
        return sb.toString();
    }

    protected List<List<Integer>> expectedRows() {
        List<List<Integer>> sorted = new ArrayList<>(DB);
        Collections.sort(sorted, rowComparator);
        return sorted;
    }

    public static Collection<List<OrderByOptions>> orderByPermutations() {
        List<Set<OrderByOptions>> optSets = new ArrayList<>();
        for(int i = 0; i < TOTAL_COLS; ++i) {
            optSets.add(new HashSet<>(Arrays.asList(OrderByOptions.values())));
        }
        return Sets.cartesianProduct(optSets);
    }

    @Parameters(name="{0}")
    public static List<Object[]> orderings() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for(List<OrderByOptions> p : orderByPermutations()) {
            String name = makeTestName(p);
            params.add(new Object[]{ name, p });
        }
        return params;
    }

    private static String makeTestName(List<OrderByOptions> orderings) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < orderings.size(); ++i) {
            String oStr = orderings.get(i).getOrderingString();
            if(oStr != null) {
                if(sb.length() > 0) {
                    sb.append('_');
                }
                sb.append(COLUMNS.get(i));
                sb.append('_').append(oStr);
            }
        }
        if(sb.length() == 0) {
            sb.append("no_order");
        }
        return sb.toString();
    }
}
