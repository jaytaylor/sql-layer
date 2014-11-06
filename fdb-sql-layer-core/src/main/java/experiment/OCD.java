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

package experiment;

// Obsessive-compulsive driver

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class OCD
{
    public static void main(String[] args) throws Exception
    {
        new OCD(args).run();
    }

    private void run() throws Exception
    {
        loadDriver();
        readQuery();
        startQueryRatePrinter();
        commandLoop();
    }

    private void loadDriver() throws ClassNotFoundException
    {
        Class.forName(DRIVER_CLASS_NAME);
    }

    private void readQuery() throws IOException
    {
        StringBuilder buffer = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        String line;
        while ((line = reader.readLine()) != null) {
            buffer.append(line);
        }
        queries = buffer.toString().split(";");
    }

    public void startQueryRatePrinter()
    {
        queryRateMonitor = new QueryRateMonitor();
        queryRateMonitor.setDaemon(true);
        queryRateMonitor.start();
    }

    private void commandLoop() throws IOException, InterruptedException, SQLException
    {
        boolean done = false;
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        do {
            System.out.print(PROMPT);
            String line = input.readLine().trim();
            if (line.equals("quit")) {
                report("*** quit");
                done = true;
                adjustThreads(0);
            } else if (line.equals("shh")) {
                quiet = true;
            } else if (line.equals("unshh")) {
                quiet = false;
            } else {
                int newThreads = 0;
                try {
                    newThreads = Integer.parseInt(line);
                } catch (NumberFormatException e) {
                    System.err.println(String.format("Not a number: %s", line));
                }
                if (newThreads < 0) {
                    System.err.println(String.format("Invalid number of threads: %s", newThreads));
                } else {
                    if (threads.size() > 0) {
                        printSummary();
                    }
                    report(String.format("*** threads: %s", newThreads));
                    adjustThreads(newThreads);
                }
            }
        } while (!done);
    }

    private void adjustThreads(int newThreads) throws InterruptedException, SQLException
    {
        while (newThreads > threads.size()) {
            QueryThread queryThread = new QueryThread();
            queryThread.start();
            threads.add(queryThread);
        }
        while (newThreads < threads.size()) {
            QueryThread queryThread = threads.remove(threads.size() - 1);
            queryThread.go();
            queryThread.shutdown();
            queryThread.join();
        }
        queryRateMonitor.reset();
        for (QueryThread thread : threads) {
            thread.go();
        }
    }

    private void printSummary()
    {
        for (QueryThread thread : threads) {
            thread.pause();
        }
        int allThreadsQueryCount = 0;
        long allThreadsTotalMsec = 0;
        int allThreadsMinMsec = Integer.MAX_VALUE;
        int allThreadsMaxMsec = Integer.MIN_VALUE;
        for (QueryThread thread : threads) {
            allThreadsQueryCount += thread.queryCount;
            allThreadsTotalMsec += thread.totalMsec;
            allThreadsMinMsec = min(allThreadsMinMsec,  thread.minMsec);
            allThreadsMaxMsec = max(allThreadsMaxMsec, thread.maxMsec);
        }
        double averageMsec = ((double) allThreadsTotalMsec) / allThreadsQueryCount;
        report(String.format("threads: %s\tmin, average, max (msec):\t%s\t%s\t%s",
                             threads.size(), allThreadsMinMsec, averageMsec, allThreadsMaxMsec));
    }

    private void report(String message)
    {
        output.println(message);
        output.flush();
    }

    private OCD(String[] args) throws IOException
    {
        int a = 0;
        try {
            String host = args[a++];
            String schema = args[a++];
            queryFile = new File(args[a++]);
            File outputFile = new File(args[a++]);
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
            output = new PrintWriter(new FileWriter(outputFile));
            url = String.format(URL_TEMPLATE, host, schema);
        } catch (Exception e) {
            System.out.println(USAGE);
        }
    }

    private static final String USAGE = "java ... experiment.OCD HOST SCHEMA QUERY_FILE OUTPUT_FILE";
    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
    private static final String URL_TEMPLATE = "jdbc:postgresql://%s:15432/%s";
    private static final String PROMPT = "> ";
    private static final int REPORTING_INTERVAL_MSEC = 1000;

    private File queryFile;
    private PrintWriter output;
    private String url;
    private String[] queries;
    private final List<QueryThread> threads = new ArrayList<>();
    private QueryRateMonitor queryRateMonitor;
    private boolean quiet = false;

    private class QueryThread extends Thread
    {
        public void run()
        {
            while (!shutdown) {
                try {
                    synchronized (this) {
                        while (paused) {
                            try {
                                wait();
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                    runQueries();
                    queryRateMonitor.queryCompleted();
                } catch (SQLException e) {
                    System.err.println(String.format("Caught error running query"));
                }
            }
            try {
                for (PreparedStatement statement : statements) {
                    statement.close();
                }
                connection.close();
            } catch (SQLException e) {
                System.err.println(String.format("Caught error shutting down: %s", e.getMessage()));
            }
        }

        public synchronized void pause()
        {
            paused = true;
        }

        public synchronized void go()
        {
            queryCount = 0;
            totalMsec = 0;
            minMsec = Integer.MAX_VALUE;
            maxMsec = Integer.MIN_VALUE;
            paused = false;
            notify();
        }

        public synchronized void shutdown()
        {
            paused = false;
            shutdown = true;
        }

        public QueryThread() throws SQLException
        {
            connection = DriverManager.getConnection(url);
            statements = new PreparedStatement[queries.length];
            for (int q = 0; q < queries.length; q++) {
                statements[q] = connection.prepareStatement(queries[q]);
            }
        }

        private void runQueries() throws SQLException
        {
            long start = System.currentTimeMillis();
            for (PreparedStatement statement : statements) {
                boolean hasResultSet = statement.execute();
                if (hasResultSet) {
                    ResultSet resultSet = statement.getResultSet();
                    while (resultSet.next());
                    resultSet.close();
                }
            }
            queryCount++;
            int msec = (int) (System.currentTimeMillis() - start);
            totalMsec += msec;
            if (msec < minMsec) {
                minMsec = msec;
            }
            if (msec > maxMsec) {
                maxMsec = msec;
            }
        }

        private Connection connection;
        private PreparedStatement[] statements;
        private volatile boolean shutdown = false;
        private volatile boolean paused = true;
        private volatile int queryCount;
        private volatile long totalMsec;
        private volatile int minMsec;
        private volatile int maxMsec;
    }

    private class QueryRateMonitor extends Thread
    {
        public void run()
        {
            while (true) {
                count = 0;
                long intervalStart = System.currentTimeMillis();
                try {
                    Thread.sleep(REPORTING_INTERVAL_MSEC);
                } catch (InterruptedException e) {
                    System.err.println("InterruptedException?!");
                }
                long intervalStop = System.currentTimeMillis();
                intervals++;
                double queriesPerSec;
                double runningAverage;
                synchronized (this) {
                    queriesPerSec = (1000.0 * count) / (intervalStop - intervalStart);
                    total += count;
                    runningAverage = (1000.0 * total) / (intervalStop - start);
                }
                if (!quiet) {
                    report(String.format("%s: queries/sec: %s\t\t%s", intervals, queriesPerSec, runningAverage));
                }
            }
        }

        public synchronized void queryCompleted()
        {
            count++;
        }

        public synchronized void reset()
        {
            start = System.currentTimeMillis();
            intervals = 0;
            total = 0;
        }

        public QueryRateMonitor()
        {
            reset();
        }

        private volatile int count = 0;
        private volatile int intervals = 0;
        private volatile long start;
        private volatile long total;
    }
}
