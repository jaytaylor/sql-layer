package com.akiban.sql.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

public abstract class GenericCreator {

    public static final String eol = System.getProperty("line.separator");

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
        String output_str = null;
        //System.out.println(sql);
        PreparedStatement stmt = conn.prepareStatement(sql);
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                stmt.setString(i + 1, args[i]);
            }
        }
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
                output_str = output_str.substring(0, output_str.length()-1);
            }
        } else {
            int count = stmt.getUpdateCount();
            //System.out.println(count + " rows updated.");
        }
        stmt.close();
        conn.close();
        return "- output: [" + output_str + "]"
                + System.getProperty("line.separator");
    }

    protected String turnArrayToCommaDelimtedList(ArrayList<String> arrayList) {
        StringBuilder sb = new StringBuilder();
        for (String s : arrayList) {
            sb.append(s);
            sb.append(",");
        }
        String retVal = sb.toString();
        return retVal.substring(0, retVal.length() - 1);
    }

    protected String trimOuterComma(String fields1) {
        String retVal = fields1.trim();
        if (retVal.endsWith(",")) {
            retVal = retVal.substring(0, retVal.length() - 1);
        }
        return retVal.trim();
    }

}
