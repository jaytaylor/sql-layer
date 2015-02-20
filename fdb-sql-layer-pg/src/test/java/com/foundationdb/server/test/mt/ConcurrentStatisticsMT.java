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

import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.util.Exceptions;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class ConcurrentStatisticsMT extends PostgresMTBase
{
    private static final double MAX_FAIL_PERCENT = 0.25;
    private static final int LOOP_COUNT = 30;
    private static final String TABLE_NAME = "t";

    private static abstract class BaseRunnable implements Runnable {
        private final PostgresMTBase testBase;
        private int retryable;

        protected BaseRunnable(PostgresMTBase testBase) {
            this.testBase = testBase;
        }

        @Override
        public void run() {
            try(Connection conn = testBase.createConnection()) {
                for(int i = 0; i < LOOP_COUNT; ++i) {
                    try {
                        runInternal(conn);
                        if(!conn.getAutoCommit()) {
                            conn.commit();
                        }
                    } catch(SQLException e) {
                        if(e.getSQLState().startsWith("40")) {
                            ++retryable;
                        } else {
                            throw new RuntimeException(e);
                        }
                        if(!conn.getAutoCommit()) {
                            conn.rollback();
                        }
                    }
                }
            } catch(Exception e) {
                throw Exceptions.throwAlways(e);
            }
        }

        public synchronized int getRetryableCount() {
            return retryable;
        }

        protected abstract void runInternal(Connection conn) throws Exception;
    }

    private static class InsertRunnable extends BaseRunnable
    {
        private PreparedStatement ps;
        private int id;

        private InsertRunnable(PostgresMTBase testBase) {
            super(testBase);
            this.id = 1;
        }

        @Override
        public void runInternal(Connection conn) throws Exception {
            if(ps == null) {
                ps = conn.prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES (?,?)");
                conn.setAutoCommit(false);
            }
            for(int i = 0; i < 50; ++i, ++id) {
                ps.setInt(1, id);
                ps.setString(2, Integer.toString(id));
                ps.executeUpdate();
            }
        }
    }

    private static class UpdateStatisticsRunnable extends BaseRunnable {
        private PreparedStatement s;

        protected UpdateStatisticsRunnable(PostgresMTBase testBase) {
            super(testBase);
        }

        @Override
        protected void runInternal(Connection conn) throws Exception {
            if(s == null) {
                s = conn.prepareStatement("ALTER TABLE "+ TABLE_NAME +" ALL UPDATE STATISTICS");
                conn.setAutoCommit(true);
            }
            s.executeUpdate();
        }
    }

    @Test
    public void concurrentInsertAndStatistics() {
        createTable(SCHEMA_NAME, TABLE_NAME, "id INT NOT NULL PRIMARY KEY, s VARCHAR(32)");
        createIndex(SCHEMA_NAME, TABLE_NAME, "s", "s");

        InsertRunnable insert = new InsertRunnable(this);
        UpdateStatisticsRunnable stats = new UpdateStatisticsRunnable(this);
        List<Thread> threads = Arrays.asList(new Thread(insert), new Thread(stats));
        ThreadHelper.runAndCheck(60000, threads);

        double total = insert.getRetryableCount() + stats.getRetryableCount();
        double percent = total / (LOOP_COUNT * 2);
        if(percent > MAX_FAIL_PERCENT) {
            fail(String.format("Too many failures: %g%% > %g%%", percent*100, MAX_FAIL_PERCENT*100));
        }
    }
}
