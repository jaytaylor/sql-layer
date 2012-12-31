/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.test;

import java.sql.*;
import java.util.*;

public class SQLClient implements Runnable
{
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: SQLClient driver url user password sql params...");
            String uname = System.getProperty("user.name", "user");
            System.out.println("e.g. 'org.postgresql.Driver' 'jdbc:postgresql://localhost:15432/" + uname + "' '" + uname + "' '" + uname + "' 'SELECT * FROM customers'");
            System.exit(1);
        }
        int argi = 0;
        String driver, url, user, password, sql;
        List<String> params;
        driver = args[argi++];
        url = args[argi++];
        user = args[argi++];
        password = args[argi++];
        sql = args[argi++];
        int repeat = 0, nthreads = 0;
        boolean printAllTimes = false;
        params = new ArrayList<String>();
        while (argi < args.length) {
            if ("--repeat".equals(args[argi])) {
                repeat = Integer.valueOf(args[argi+1]);
                argi += 2;
            }
            else if ("--threads".equals(args[argi])) {
                nthreads = Integer.valueOf(args[argi+1]);
                argi += 2;
            }
            else if ("--times".equals(args[argi])) {
                printAllTimes = true;
                argi += 1;
            }
            else {
                params.add(args[argi]);
            }
        }
        if (printAllTimes && nthreads > 1) {
            System.err.println(String.format("--times is incompatible with --threads %s", nthreads));
            return;
        }
        Class.forName(driver);
        System.out.println(sql);
        long total;
        if ((repeat == 0) || (nthreads == 0)) {
            SQLClient client = new SQLClient(url, user, password, sql,
                                             params, 0, repeat, printAllTimes);
            client.run();
            total = client.time();
        }
        else {
            new SQLClient(url, user, password, sql, params, 0, 0, false).run();

            SQLClient[] clients = new SQLClient[nthreads];
            Thread[] threads = new Thread[nthreads];
            for (int i = 0; i < nthreads; i++) {
                clients[i] = new SQLClient(url, user, password, sql, params, 1, repeat, false);
                threads[i] = new Thread(clients[i]);
            }
            for (int i = 0; i < nthreads; i++) {
                threads[i].start();
            }
            total = 0;
            for (int i = 0; i < nthreads; i++) {
                threads[i].join();
                total += clients[i].time();
            }
        }
        if (repeat > 0) {
            total = total / (repeat * ((nthreads == 0) ? 1 : nthreads));
            System.out.println(total + " ms.");
        }
    }

    private String url, user, password, sql;
    private List<String> params;
    private int start, repeat;
    private boolean printAllTimes;
    private long time;

    public SQLClient(String url, String user, String password, String sql,
                     List<String> params, int start, int repeat, boolean printAllTimes) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.sql = sql;
        this.params = params;
        this.start = start;
        this.repeat = repeat;
        this.printAllTimes = printAllTimes;
    }
        
    public long time() {
        return time;
    }

    @Override
    public void run() {
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            PreparedStatement stmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.size(); i++) {
                stmt.setString(i + 1, params.get(i));
            }
            long startTime = -1, endTime, queryStartTime = -1, queryEndTime;
            for (int pass = start; pass <= repeat; pass++) {
                if (pass == 1) startTime = System.currentTimeMillis();
                if (printAllTimes) queryStartTime = System.currentTimeMillis();
                if (stmt.execute()) {
                    ResultSet rs = stmt.getResultSet();
                    ResultSetMetaData md = rs.getMetaData();
                    if (pass == 0) {
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            if (i > 1) System.out.print("\t");
                            System.out.print(md.getColumnName(i));
                        }
                        System.out.println();
                    }
                    while (rs.next()) {
                        if (pass > 0) continue;
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            if (i > 1) System.out.print("\t");
                            System.out.print(rs.getString(i));
                        }
                        System.out.println();
                    }
                }
                else {
                    int count = stmt.getUpdateCount();
                    if (pass == 0)
                        System.out.println(count + " rows updated.");
                }
                if (printAllTimes) {
                    queryEndTime = System.currentTimeMillis();
                    System.out.println(String.format("%s: pass %s: %s msec",
                                                     queryEndTime, pass, queryEndTime - queryStartTime));
                }
            }
            endTime =  System.currentTimeMillis();
            stmt.close();
            conn.close();
            time = endTime - startTime;
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
