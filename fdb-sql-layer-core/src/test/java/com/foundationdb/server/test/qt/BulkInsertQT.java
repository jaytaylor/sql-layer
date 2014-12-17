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

package com.foundationdb.server.test.qt;

import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;
import com.foundationdb.server.store.FDBPendingIndexChecks.CheckTime;
import com.foundationdb.sql.server.ServerSession;

import java.sql.*;
import java.util.*;

import org.junit.Before;
import org.junit.Test;

public class BulkInsertQT extends EmbeddedJDBCITBase
{
    @Before
    public void populate() throws SQLException {
        createTable(SCHEMA_NAME, "t1", "id INT PRIMARY KEY, n1 INT, s1 VARCHAR(128)");
    }

    static class TestParameters {
        boolean usePreparedStatements;
        int totalRows, rowsPerTransaction, rowsPerStatement, nthreads;
        CheckTime checkTime;

        public boolean isAutoCommit() {
            return (rowsPerTransaction < 0);
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder();
            str.append("total rows = ").append(totalRows).append("\n");
            if (checkTime != null) {
                str.append("check time = ").append(checkTime).append("\n");;
            }
            if (usePreparedStatements)
                str.append("prepared statements\n");
            else
                str.append("no prepared statements\n");
            if (isAutoCommit())
                str.append("auto-commit\n");
            else
                str.append("rows / transaction = ").append(rowsPerTransaction).append("\n");
            str.append("rows / statement = ").append(rowsPerStatement).append("\n");;
            str.append("threads = ").append(nthreads).append("\n");;
            return str.toString();
        }

        static final int NPARAMS = 5;

        public int countChanges(TestParameters other) {
            int count = 0;
            if (usePreparedStatements != other.usePreparedStatements)
                count++;
            if (rowsPerTransaction != other.rowsPerTransaction)
                count++;
            if (rowsPerStatement != other.rowsPerStatement)
                count++;
            if (nthreads != other.nthreads)
                count++;
            if (checkTime != other.checkTime)
                count++;
            return count;
        }
    }

    class TestRunner implements Runnable {
        final TestParameters params;
        final int startRow, endRow;
        final Random random = new Random();

        Connection connection;
        Statement statement;
        PreparedStatement preparedStatement;
        int preparedCount;

        Exception error;
        long nanoTime;
        int retries;
        
        public TestRunner(TestParameters params) {
            this(params, 1, 1 + params.totalRows);
        }

        public TestRunner(TestParameters params, int startRow, int endRow) {
            this.params = params;
            this.startRow = startRow;
            this.endRow = endRow;
        }

        protected void openConnection() throws SQLException {
            connection = getConnection();
            statement = connection.createStatement();
            if (!params.isAutoCommit())
                connection.setAutoCommit(false);
            if (params.checkTime != null) {
                if (connection instanceof ServerSession) {
                    ((ServerSession)connection).setProperty("constraintCheckTime",
                                                            params.checkTime.toString());
                }
                else {
                    statement.execute(String.format("SET constraintCheckTime = '%s'",
                                                    params.checkTime));
                }
            }
        }

        protected void doInserts() throws SQLException {
            statement.executeUpdate("TRUNCATE TABLE t1");
            if (!params.isAutoCommit()) {
                connection.commit();
            }

            long startTime = System.nanoTime();

            int rowsInTransaction = 0;
            int row = startRow;
            while (row < endRow) {
                int rowsToDo = params.rowsPerStatement;
                if (row + rowsToDo > endRow) {
                    rowsToDo = endRow - row;
                }
                do {
                    try {
                        int ninserted;
                        if (!params.usePreparedStatements) {
                            String sql = buildInsertSQL(rowsToDo, false);
                            Object[] params = new Object[rowsToDo * 3];
                            int n = 0;
                            for (int i = 0; i < rowsToDo; i++) {
                                params[n++] = row + i;
                                params[n++] = random.nextInt(10000);
                                params[n++] = randomString();
                            }
                            sql = String.format(sql, params);
                            ninserted = statement.executeUpdate(sql);
                        }
                        else {
                            if (preparedCount != rowsToDo) {
                                if (preparedStatement != null) {
                                    preparedStatement.close();
                                    preparedStatement = null;
                                }
                                String sql = buildInsertSQL(rowsToDo, true);
                                preparedStatement = connection.prepareStatement(sql);
                                preparedCount = rowsToDo;
                            }
                            int n = 1;
                            for (int i = 0; i < rowsToDo; i++) {
                                preparedStatement.setInt(n++, row + i);
                                preparedStatement.setInt(n++, random.nextInt(10000));
                                preparedStatement.setString(n++, randomString());
                            }
                            ninserted = preparedStatement.executeUpdate();
                        }
                        assert(ninserted == rowsToDo);
                        if (!params.isAutoCommit()) {
                            rowsInTransaction += rowsToDo;
                            if ((rowsInTransaction >= params.rowsPerTransaction) ||
                                (row + rowsToDo >= endRow)) {
                                connection.commit();
                                rowsInTransaction = 0;
                            }
                        }
                        break;
                    }
                    catch (SQLException ex) {
                        if (ex.getSQLState().startsWith("40")) {
                            retries++;
                            continue;
                        }
                        throw ex;
                    }
                } while (false);
                row += rowsToDo;
            }
            long endTime = System.nanoTime();
            nanoTime = endTime - startTime;
        }

        protected String buildInsertSQL(int nrows, boolean qmark) {
            StringBuilder str = new StringBuilder("INSERT INTO t1 VALUES");
            for (int i = 0; i < nrows; i++) {
                if (i > 0) str.append(",");
                str.append(qmark ? "(?,?,?)" : "(%d,%d,'%s')");
            }
            return str.toString();
        }

        protected String randomString() {
            int size = 100 + random.nextInt(100);
            char[] chars = new char[size];
            for (int i = 0; i < size; i++) {
                chars[i] = (char)('A' + random.nextInt(26));
            }
            return new String(chars);
        }

        @Override
        public void run() {
            try {
                openConnection();
                doInserts();
            }
            catch (Exception ex) {
                error = ex;
            }
            finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    }
                    catch (SQLException ex) {
                    }
                }
                if (statement != null) {
                    try {
                        statement.close();
                    }
                    catch (SQLException ex) {
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    }
                    catch (SQLException ex) {
                    }
                }
            }
        }
    }

    class TestResult {
        final TestParameters params;
        double rowsPerSecond;
        int totalRetries;
        
        public TestResult(TestParameters params) {
            this.params = params;
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder(params.toString());
            str.append("rows / second = ").append(rowsPerSecond).append("\n");
            str.append("total retries = ").append(totalRetries).append("\n");
            return str.toString();
        }

        public void compute() throws Exception {
            long maxTime = 0;
            if (params.nthreads == 1) {
                TestRunner runner = new TestRunner(params);
                runner.run();
                if (runner.error != null) throw runner.error;
                maxTime = runner.nanoTime;
                totalRetries = runner.retries;
            }
            else {
                TestRunner[] runners = new TestRunner[params.nthreads];
                Thread[] threads = new Thread[params.nthreads];
                int startRow = 1, remaining = params.totalRows;
                for (int i = 0; i < params.nthreads; i++) {
                    int rows = remaining / (params.nthreads - i);
                    runners[i] = new TestRunner(params, startRow, startRow + rows);
                    startRow += rows;
                    remaining -= rows;
                    threads[i] = new Thread(runners[i]);
                }
                assert(remaining == 0);
                for (int i = 0; i < params.nthreads; i++) {
                    threads[i].start();
                }                
                for (int i = 0; i < params.nthreads; i++) {
                    threads[i].join();
                    if (runners[i].error != null)
                        throw runners[i].error;
                    if (maxTime < runners[i].nanoTime)
                        maxTime = runners[i].nanoTime;
                    totalRetries += runners[i].retries;
                }
            }
            rowsPerSecond = params.totalRows / (maxTime / 1.0e9);
        }
    }

    @Test
    public void test() throws Exception {
        int warmupCount = Integer.parseInt(System.getProperty("BULK_INSERT_WARMUP", "1000"));
        int testCount = Integer.parseInt(System.getProperty("BULK_INSERT_COUNT", "10000"));
        List<TestResult> results = new ArrayList<>();
        for (Boolean warmup : new Boolean[] {
                 Boolean.TRUE, Boolean.FALSE 
             }) {
            for (Boolean usePreparedStatements : new Boolean[] {
                     Boolean.FALSE, Boolean.TRUE
                 }) {
                for (Integer nthreads : new Integer[] { 1, 4, 8 }) {
                    for (Integer rowsPerStatements : new Integer[] { 1, 10, 100 }) {
                        for (Integer rowsPerTransaction : new Integer[] { -1, 1000 }) {
                            for (CheckTime checkTime : new CheckTime[] {
                                     CheckTime.IMMEDIATE, CheckTime.STATEMENT,
                                     CheckTime.STATEMENT_WITH_RANGE_CACHE,
                                     CheckTime.DELAYED,
                                     CheckTime.DELAYED_WITH_RANGE_CACHE
                                 }) {
                                TestParameters params = new TestParameters();
                                params.totalRows = warmup ? warmupCount : testCount;
                                params.usePreparedStatements = usePreparedStatements;
                                params.nthreads = nthreads;
                                params.rowsPerStatement = rowsPerStatements;
                                params.rowsPerTransaction = rowsPerTransaction;
                                params.checkTime = checkTime;
                                TestResult result = new TestResult(params);
                                System.out.print(".");
                                try {
                                    result.compute();
                                    if (!warmup) results.add(result);
                                }
                                catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println();
        Collections.sort(results,
                         new Comparator<TestResult>() {
                             @Override
                             public int compare(TestResult r1, TestResult r2) {
                                 if (r1.rowsPerSecond < r2.rowsPerSecond)
                                     return -1;
                                 else if (r1.rowsPerSecond > r2.rowsPerSecond)
                                     return +1;
                                 else
                                     return 0;
                             }
                         });
        for (TestResult result : results) {
            System.out.println(result);
        }
        if (false) {
            System.out.println("=====");
            reportImprovements(results);
        }
    }

    // Simulated hill climbing -- less informative than hoped.
    protected void reportImprovements(List<TestResult> results) {
        int[] better = new int[TestParameters.NPARAMS];
        TestResult prev = null;
        int pos = 0;
        while (true) {
            TestResult current = results.get(pos);
            System.out.println();
            System.out.print(current);
            TestParameters params = current.params;
            if (prev != null) {
                int diffs = params.countChanges(prev.params);
                if (diffs > 1) {
                    System.out.println("changed parameters = " + diffs);
                }
                System.out.println("improvement = " +
                                   current.rowsPerSecond / prev.rowsPerSecond);
            }
            Arrays.fill(better, -1);
            int next = pos + 1;
            if (next >= results.size()) break;
            while (next < results.size()) {
                better[params.countChanges(results.get(next).params) - 1] = next;
                next++;
            }
            for (int b : better) {
                if (b >= 0) {
                    pos = b;
                    break;
                }
            }
            prev = current;
        }
    }
    
}
