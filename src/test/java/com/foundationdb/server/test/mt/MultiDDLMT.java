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

package com.foundationdb.server.test.mt;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** Multiple threads executing non-overlapping DDL. */
@RunWith(NamedParameterizedRunner.class)
public class MultiDDLMT extends PostgresMTBase
{
    private static final int LOOPS = 10;

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        return Arrays.asList(Parameterization.create("oneThread", 1),
                             Parameterization.create("twoThreads", 2),
                             Parameterization.create("fiveThreads", 5));
    }

    private static final class DDLWorker extends Thread
    {
        private final String schema;
        private final Connection conn;
        private Statement s;

        public DDLWorker(String schema, Connection conn) {
            super("DDL_"+schema);
            this.schema = schema;
            this.conn = conn;
        }

        @Override
        public void run() {
            String table = String.format("\"%s\".t", schema);
            String[] queries = {
                "CREATE TABLE "+table+"(id INT NOT NULL PRIMARY KEY, x INT)",
                "INSERT INTO "+table+" VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
                "CREATE INDEX x ON "+table+"(x)",
                "DROP TABLE "+table
            };
            try {
                for(int i = 0; i < LOOPS; ++i) {
                    for(String q : queries) {
                        execQuery(q);
                        delay();
                    }
                }
            } finally {
                try {
                    conn.close();
                } catch(SQLException e) {
                    // Ignore
                }
            }
        }

        private void delay() {
            try {
                Thread.sleep(10);
            } catch(InterruptedException e) {
                // Ignore
            }
        }

        private void execQuery(String query) {
            for(;;) {
                try {
                    if(s == null) {
                        s = conn.createStatement();
                    }
                    s.execute(query);
                    break;
                } catch(SQLException e) {
                    ErrorCode code = ErrorCode.valueOfCode(e.getSQLState());
                    if(code == ErrorCode.STALE_STATEMENT) {
                        // retry with new statement
                        try {
                            s.close();
                        } catch(SQLException e1) {
                            // Ignore
                        }
                        s = null;
                    } else if(code.isRollbackClass()) {
                        // retry after slight delay
                        delay();
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private final int threadCount;

    public MultiDDLMT(int threadCount) {
        this.threadCount = threadCount;
    }

    @Test
    public void runTest() throws Exception {
        List<DDLWorker> threads = new ArrayList<>(threadCount);
        for(int i = 0; i < threadCount; ++i) {
            threads.add(new DDLWorker("test_" + i, createConnection()));
        }
        ThreadHelper.runAndCheck(60000, threads);
        for(DDLWorker w : threads) {
            w.conn.close();
        }
    }
}
