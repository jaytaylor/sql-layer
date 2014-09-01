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

package com.foundationdb.sql.pg;

import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PostgresServerLargeSortKeysIT extends PostgresServerFilesITBase {
    private final static int MAX_STRING_LENGTH = 2500;

    private final static int D = 2;
    private final static int[] LENGTHS = { 60, 200, 255, 256, 257, MAX_STRING_LENGTH };
    private final static int[] POSITIONS = { 50, -50 };
    private final static Boolean[] CASE_CONVERSIONS = { false, true };
    private final static String[] REPLACEMENTS = { " Cat ", " Mouse " };

    private final static int L = LENGTHS.length;
    private final static int P = POSITIONS.length;
    private final static int R = REPLACEMENTS.length;
    private final static int C = CASE_CONVERSIONS.length;

    private final static int T = D * L * L * L * L * P * R * C;

    private final static String SQL_ALL = "select a, b, c, d from t1";
    private final static String SQL_DISTINCT_ALL = "select distinct a, b, c, d from t1";
    private final static String SQL_DISTINCT_AB = "select distinct a, b from t1";
    private final static String SQL_DISTINCT_CD = "select distinct c, d from t1";
    private final static String SQL_DISTINCT_BCD = "select distinct b, c, d from t1";

    private final static String BIG_STRING;
    static {
        StringBuilder sb = new StringBuilder(MAX_STRING_LENGTH);
        for (int i = 0; i < MAX_STRING_LENGTH; i++) {
            sb.append((char) ('A' + (i % 25)));
        }
        BIG_STRING = sb.toString();
    }

    @Before
    public void loadDatabase() throws Exception {
        createTable(SCHEMA_NAME, "t1", "id int NOT NULL", "PRIMARY KEY(id)",
                "a varchar(65535) CHARACTER SET latin1 COLLATE en_us_ci NOT NULL",
                "b varchar(65535) CHARACTER SET latin1 COLLATE en_us_ci NOT NULL",
                "c varchar(65535) CHARACTER SET latin1 COLLATE utf8_bin NOT NULL",
                "d varchar(65535) CHARACTER SET latin1 COLLATE utf8_bin NOT NULL");

        PreparedStatement stmt = getConnection().prepareStatement("insert into t1 (id, a, b, c, d) values (?,?,?,?,?)");
        getConnection().setAutoCommit(false);
        /*
         * Insert 3 sets of duplicate rows
         */
        int count = 0;
        for (int dup = 0; dup < D; dup++) {
            for (int a : LENGTHS) {
                getConnection().commit();
                for (int b : LENGTHS) {
                    for (int c : LENGTHS) {
                        for (int d : LENGTHS) {
                            for (int p : POSITIONS) {
                                for (String s : REPLACEMENTS) {
                                    for (boolean cc : CASE_CONVERSIONS) {
                                        stmt.setInt(1, ++count);
                                        stmt.setString(2, strVal(a, s, p > 0 ? p : a + p, cc));
                                        stmt.setString(3, strVal(b, s, p > 0 ? p : b + p, cc));
                                        stmt.setString(4, strVal(c, s, p > 0 ? p : c + p, cc));
                                        stmt.setString(5, strVal(d, s, p > 0 ? p : d + p, cc));
                                        stmt.execute();
                                        assertEquals("Insert failed", 1, stmt.getUpdateCount());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        getConnection().commit();
        getConnection().setAutoCommit(true);
    }

    private String strVal(int len, String replace, int position, boolean caseConvert) {
        StringBuilder sb = new StringBuilder(BIG_STRING);
        sb.replace(position, position + replace.length(), replace);
        String s = sb.substring(0, len);
        if (caseConvert) {
            s = s.toLowerCase();
        }
        return s;
    }

    @After
    public void dontLeaveConnection() throws Exception {
        // Tests change read only state. Easiest not to reuse.
        forgetConnection();
    }

    @Test
    public void countRows() throws Exception {
        assertEquals("Missing rows", T, countRows(SQL_ALL));
        assertEquals("Distinct rows", T / D, countRows(SQL_DISTINCT_ALL));
        assertEquals("Distinct rows", T / D / L / L, countRows(SQL_DISTINCT_CD));
        assertEquals("Distinct rows", T / D / L / L / C, countRows(SQL_DISTINCT_AB));
        assertEquals("Distinct rows", T / D / L, countRows(SQL_DISTINCT_BCD));
    }

    public int countRows(final String sql) throws Exception {

        Statement stmt = getConnection().createStatement();
        int count = 0;
        try {
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                count++;
            }
        } finally {
            stmt.close();
        }
        return count;
    }

}
