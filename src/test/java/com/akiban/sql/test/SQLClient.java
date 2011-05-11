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
        Class.forName(args[0]);
        Connection conn = DriverManager.getConnection(args[1], args[2], args[3]);
        String sql = args[4];
        System.out.println(sql);
        PreparedStatement stmt = conn.prepareStatement(sql);
        for (int i = 5; i < args.length; i++) {
            stmt.setString(i - 4, args[i]);
        }
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            if (i > 1) System.out.print("\t");
            System.out.print(md.getColumnName(i));
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) System.out.print("\t");
                System.out.print(rs.getString(i));
            }
            System.out.println();
        }
        stmt.close();
        conn.close();
    }
}
