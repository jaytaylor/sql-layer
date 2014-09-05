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
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.sql.pg.PostgresServerITBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(SelectedParameterizedRunner.class)
public class IndexScanUnboundedMixedOrderDT extends PostgresServerITBase {

    static final Integer TOTAL_ROWS = 100;
    static final Integer TOTAL_COLS = 4;
    static final Integer TOTAL_PERMS = 64;

    static final String QUERY_BASE = "SELECT * FROM test.t ";
    static final String[] COLUMNS = {"t0", "t1", "t2", "t3"};
    static final String[] OPTIONS = {"", "ASC", "DESC"};

    String[] orderings;
    String query = null;
    
    Integer[][] DB;

    static final long seed = System.currentTimeMillis();
    static final Random r = new Random(seed);

    public IndexScanUnboundedMixedOrderDT(String t0, String t1, String t2, String t3) {
        orderings = new String[]{t0, t1, t2, t3};
        buildDB();
    }

    void buildDB() {
        DB = new Integer[TOTAL_ROWS][TOTAL_COLS + 1];
        for (int i = 0; i < TOTAL_ROWS; i++) {
            DB[i][0] = i + 1;
            for (int j = 1; j < TOTAL_COLS + 1; j++) {
                int next = r.nextInt(110);
                DB[i][j] = next > 100 ? null : next;
            }
        }
    }

    @Before
    public void setup() {
        sql("CREATE TABLE t(id INT NOT NULL PRIMARY KEY, t0 INT, t1 INT, t2 INT, t3 INT)");
        sql("CREATE INDEX t_ndx ON t(t0, t1, t2, t3, id)");
        String insertStmt = "INSERT INTO t VALUES ";
        for (int i = 0; i < DB.length; i++) {
            insertStmt = insertStmt + "(";
            for (int j = 0; j < DB[i].length; j++) {
                if (DB[i][j] == null) insertStmt = insertStmt + "null";
                else insertStmt = insertStmt + DB[i][j].toString();
                if (j != DB[i].length - 1) insertStmt = insertStmt + ","; 
            }
            insertStmt = insertStmt + "),";
        }
        insertStmt = insertStmt.substring(0, insertStmt.length()-1);
        sql(insertStmt);
    }

    @Test
    public void testUnboundedExhaustive() {
        query = createQuery();
        List<List<?>> results = sql(query);
        compare(expectedRows(), results);
    }

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
            }
        }
    }

    String createQuery() {
        String query = QUERY_BASE + " ORDER BY ";
        for (int i = 0; i < orderings.length; i++) {
            if (!orderings[i].equals("")) {
                query = query + COLUMNS[i] + " " + orderings[i] + ", ";
            }
        }
        return query.substring(0, query.length()-2) + ";";
    }

    Integer[][] expectedRows() {
        return sortRows();
    }

    Integer[][] sortRows() {
        Integer[][] sorted = DB.clone();
        Arrays.sort(sorted, new Comparator<Integer[]>() {
            public int compare(Integer[] o1, Integer[] o2) {
                // first pass, only pay attention to ordered
                for (int i = 0; i < orderings.length; i++) {
                    if (orderings[i].isEmpty()) continue;
                    if (o1[i+1] == null && o2[i+1] == null) continue;
                    else if (o1[i+1] == null) return orderings[i].equals("ASC") ? -1 : 1;
                    else if (o2[i+1] == null) return orderings[i].equals("ASC") ? 1 : -1;
                    if (((int)o1[i+1]) < ((int)o2[i+1])) {
                        return orderings[i].equals("ASC") ? -1 : 1;
                    }
                    if (((int)o1[i+1]) > ((int)o2[i+1])) {
                        return orderings[i].equals("ASC") ? 1 : -1;
                    }
                }
                return 0;
            }
        });
        return sorted;
    }

    @Parameters
    public static Iterable<Object[]> orderings() throws Exception {
        Collection<Object[]> params = new ArrayList<>();
        String[] list;
        for (int i = 1; i < TOTAL_PERMS; i++) {
            list = getPermutation(OPTIONS, i);
            params.add(list);
        }
        return params;
    }

    static String[] getPermutation(String[] options, int ndx) {
        String[] nextRow = new String[TOTAL_COLS];
        boolean end = true;
        for (int i = 0; i < TOTAL_COLS; i++) {
            String next = options[ndx / ((int) Math.pow(options.length, i)) % options.length];
            nextRow[i] = next;
            if (next.compareTo("") != 0) end = false; 
        }
        if (end) nextRow[0] = "ASC"; // prevent case with no orderings
        return nextRow;
    }
}
