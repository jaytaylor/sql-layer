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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Locale;

/*
 * General class for support of iterating through sql schema variations
 * This class should be extended for specific tests sets and schemas
 * */
public abstract class GenericCreator {

    public static final String eol = System.getProperty("line.separator");
    public static String TARGET_AREA = "query-combo";
    public static final String STR_METHOD = "[s]=";
    public static final String DT_METHOD = "[d]=";
    public static final String INT_METHOD = "[i]=";
    public final String[] QUANTIFIERS = { "Distinct", "All", "" };
    public final String[] FUNCTION_LIST = {
            "[s]=%1$s = '%2$s'",
            "[d]=%1$s = %2$s", "[i]=%1$s = '%2$s'" };
    public final String[] AG_FUNCTION_LIST = { "[i]=SUM(%1$s)",
            "[i]=AVG(%1$s)", "[i]=COUNT(%1$s)", "[i]=MIN(%1$s)",
            "[i]= MAX(%1$s)", "[i]=COUNT(*)" };
    protected StringBuilder sb = new StringBuilder();
    protected Formatter formatter = new Formatter(sb, Locale.US);
    String path = System.getProperty("user.dir")
            + "/src/test/resources/com/akiban/sql/pg/yaml/functional/";
    protected String server = "localhost";
    protected String username = "root";
    protected String password = "";
    int counter = 0;
    public static final String[] ORDER_BY_DIRECTION = { " ASC ", " DESC " };
    public static final String[] JOIN_OTHER = { " CROSS JOIN %3$s " };
    public static final String[] JOIN_NATURAL = { " NATURAL ", " " };
    public static final String[] JOIN_TYPE = { " INNER ", " LEFT ", " RIGHT ",
            " LEFT OUTER ", " RIGHT OUTER ", " " };
    public static final String[] JOIN_SPEC = { " ON %1$s.%2$s = %3$s.%4$s " };
            //" USING (%4$s) " };
    public static final String JOIN = " JOIN %3$s ";
    int empty_counter = 0;
    
    protected void close() {
        System.out.println("Empty Counter: "+empty_counter);
        
    }
    
    protected void save(String filename, StringBuilder data) throws IOException {
        try {
            // Create file
            FileWriter fstream = new FileWriter(filename);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(data.toString());
            // Close the output stream
            out.close();
        } catch (Exception e) {// Catch exception if any            
            System.err.println("Error: " + e.getMessage());
        }
        File f = new File(filename);
        //System.out.println(f.getCanonicalPath());
        //System.out.println(data.toString());

    }

    public String generateOutputFromInno(String server, String username,
            String password, String sql, String args[]) throws Exception {
        StringBuilder output = new StringBuilder();
        Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://" + server + "/test";
        Connection conn = DriverManager.getConnection(url, username, password);
        String output_str = "";
        //System.out.println("generateOutputFromInno: " + sql);
        PreparedStatement stmt = conn.prepareStatement(sql);
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                stmt.setString(i + 1, args[i]);
            }
        }
        try {
            if (stmt.execute()) {
                ResultSet rs = stmt.getResultSet();
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    output.append("[");
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        if (i > 1)
                            output.append(",");
                        output.append("'" + rs.getString(i) + "'");
                    }
                    output.append("],");

                }
                output_str = output.toString();
                if (output_str.length() > 4) {
                    output_str = output_str.substring(0,
                            output_str.length() - 1);
                }
            } 
        } finally {
            stmt.close();
            conn.close();
        }
        if (output_str == null || output_str.trim().equals("")) {
            empty_counter++;
        }
        if (output_str != null && output_str.length() > 0
                && output_str.indexOf("order by") <= 0) {
            output_str = "- output_ordered: [" + output_str + "]"
                    + System.getProperty("line.separator");
        } else {
            output_str = "- output: [" + output_str + "]"
                    + System.getProperty("line.separator");
        }

        return output_str;
    }

    protected String trimOuterComma(String fields1) {
        String retVal = fields1.trim();
        if (retVal.endsWith(",")) {
            retVal = retVal.substring(0, retVal.length() - 1);
        }
        return retVal.trim();
    }

    protected String format(int start_param_index, int function_index,
            String field, String[] source, String filter) {
        sb.setLength(0);
        formatter.format(
                new String(filterFunctionList(filter).get(function_index)),
                field, source[start_param_index],
                source[Math.min(source.length - 1, (start_param_index + 1))],
                source[Math.min(source.length - 1, (start_param_index + 2))],
                source[Math.min(source.length - 1, (start_param_index + 3))],
                source[Math.min(source.length - 1, (start_param_index + 4))]);
        String retVal = sb.toString();
        sb.setLength(0);
        return retVal;
    }

    protected ArrayList<String> filterFunctionList(String filter) {
        ArrayList<String> retVal = new ArrayList<String>();
        for (int x = 0; x < FUNCTION_LIST.length; x++) {
            if (FUNCTION_LIST[x].startsWith(filter)) {
                retVal.add(FUNCTION_LIST[x].substring(4));
            }

        }
        return retVal;
    }

    protected StringBuilder getAppender(String modifier) {
         
        return new StringBuilder();
    }

    protected boolean deleteFile(String modifier) {
        String path = System.getProperty("user.dir")
                + "/src/test/resources/com/akiban/sql/pg/yaml/functional/";
        return new File(path + "test-" + TARGET_AREA + "-" + modifier + ".yaml")
                .delete();
    }

    protected String generateMySQL(String sql) {
        // replace any syntax that is different between systems
        return sql;
    }

    protected void writeYamlBlock(StringBuilder writer, String sql) {
        try {
            writer.append("---" + eol);

            writer.append("- Statement: " + sql + eol);
            String mySQL_sql = generateMySQL(sql);
            String expected_output = callMySQL(mySQL_sql);
            writer.append(expected_output);
            //System.out.println(sql);
            counter++;

        } catch (Exception e) {
            System.out.println("ERROR(wyb):  " + e.getMessage());
        }
    }

    private String callMySQL(String sql) {
        String retVal = "";

        try {
            retVal = generateOutputFromInno(server, username, password, sql,
                    null);
            if (retVal == null) {
                retVal = "";
                throw new Exception("Result was null");
            }
        } catch (Exception e) {
            System.out.println("");
            System.out.println("MySQL: " + sql);
            System.out.println("MySQL: " + retVal);
            System.out.println("MySQL ERROR:  " + e.getMessage());
            //System.exit(-1);
            System.out.println("");
        }
        return retVal;
    }

    public static class Relationship {

        public Relationship(String primaryTable, String secondaryTable,
                String primaryKey, String secondaryKey) {
            super();
            this.primaryTable = primaryTable;
            this.secondaryTable = secondaryTable;
            this.primaryKey = primaryKey;
            this.secondaryKey = secondaryKey;
        }

        public String primaryTable;
        public String secondaryTable;
        public String primaryKey;
        public String secondaryKey;
    }
}
