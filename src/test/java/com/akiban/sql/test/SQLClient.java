/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
