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

public class SQLClient
{
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: SQLClient driver url user password sql params...");
            String uname = System.getProperty("user.name", "user");
            System.out.println("e.g. 'org.postgresql.Driver' 'jdbc:postgresql://localhost:15432/" + uname + "' '" + uname + "' '" + uname + "' 'SELECT * FROM customers'");
            System.exit(1);
        }
        int argi = 0;
        Class.forName(args[argi++]);
        Connection conn = DriverManager.getConnection(args[argi++], args[argi++], args[argi++]);
        String sql = args[argi++];
        System.out.println(sql);
        int repeat = 0;
        while (argi < args.length) {
            if ("--repeat".equals(args[argi])) {
                repeat = Integer.valueOf(args[argi+1]);
                argi += 2;
            }
            else if ("--cbo".equals(args[argi])) {
                Statement stmt = conn.createStatement();
                stmt.execute("SET cbo TO '" + args[argi+1] + "'");
                stmt.close();
                argi += 2;
            }
            else
                break;
        }
        PreparedStatement stmt = conn.prepareStatement(sql);
        long startTime = -1, endTime;
        for (int i = argi; i < args.length; i++) {
            stmt.setString(i - argi + 1, args[i]);
        }
        for (int pass = 0; pass <= repeat; pass++) {
            if (pass == 1) startTime = System.currentTimeMillis();
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
        }
        endTime =  System.currentTimeMillis();
        if (repeat > 0) 
            System.out.println((endTime - startTime) + " ms.");
        stmt.close();
        conn.close();
    }
}
