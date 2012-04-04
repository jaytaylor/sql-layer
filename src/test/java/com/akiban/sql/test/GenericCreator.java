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
import java.util.Formatter;
import java.util.Locale;

/*
 * General class for support of iterating through sql schema variations
 * This class should be extended for specific tests sets and schemas
 * */
public abstract class GenericCreator {

    public static final String eol = System.getProperty("line.separator");
    public static String targetArea = "query-combo";
    protected StringBuilder sb = new StringBuilder();
    protected Formatter formatter = new Formatter(sb, Locale.US);
    String path = System.getProperty("user.dir")
            + "/src/test/resources/com/akiban/sql/pg/yaml/functional/";
    int empty_counter = 0;
    int counter = 0;

    // See 

    // a way to determine if the function is a string, date or numeric type function
    // current plan is to repeat functions that could be more then one
    // for items in FUNCTION_LIST
    public static final String STR_METHOD = "[s]=";
    public static final String DT_METHOD = "[d]=";
    public static final String INT_METHOD = "[i]=";
    public static final String[] FUNCTION_LIST = { "[s]=%1$s = '%2$s'",
            "[d]=%1$s = %2$s", "[i]=%1$s = '%2$s'" };

    //"select " + quantifier + getFields() + from + getJoins() + getWhere() + getGroupby() + having + getOrderby() + getLimit();
    //  SELECT [ <set quantifier> ] <select list> <table expression>
    //    <table expression>    ::= 
    //            <from clause>
    //            [ <where clause> ]
    //            [ <group by clause> ]
    //            [ <having clause> ]
    // order by
    // limit 
    public static final String[] QUANTIFIERS = { "Distinct", "All", "" };

    public static final String[] ORDER_BY_DIRECTION = { " ASC ", " DESC " };

    // unique join syntax
    public static final String[] JOIN_OTHER = { " CROSS JOIN %3$s " };

    // combinations of joins in order to build
    public static final String[] JOIN_NATURAL = { " NATURAL ", " " };
    public static final String[] JOIN_TYPE = { " INNER ", " LEFT ", " RIGHT ",
            " LEFT OUTER ", " RIGHT OUTER ", " " };
    public static final String[] JOIN_SPEC = { " ON %1$s.%2$s = %3$s.%4$s ",
            " USING (%4$s) " };
    public static final String JOIN = " JOIN %3$s ";

    // unused as of yet, but lists the aggregate functions that should be used to test group by
    public static final String[] AG_FUNCTION_LIST = { "[i]=SUM(%1$s)",
            "[i]=AVG(%1$s)", "[i]=COUNT(%1$s)", "[i]=MIN(%1$s)",
            "[i]= MAX(%1$s)", "[i]=COUNT(*)" };

    // should be parameterized
    protected String server = "localhost";
    protected String username = "root";
    protected String password = "";

    protected void close() {
        System.out.println("Empty Counter: " + empty_counter);
    }

    protected void save(String filename, StringBuilder data) throws IOException {
        BufferedWriter out = null;
        try {
            // Create file
            FileWriter fstream = new FileWriter(filename);
            out = new BufferedWriter(fstream);
            out.write(data.toString());
        } finally {
            out.close();
        }
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
                    if (output.length() > 0) {
                        output.append(",");
                    }
                    output.append("[");
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        if (i > 1)
                            output.append(",");
                        output.append("'" + rs.getString(i) + "'");
                    }
                    output.append("]");

                }
                output_str = output.toString();

            }
        } finally {
            stmt.close();
            conn.close();
        }
        if (output_str == null || output_str.trim().equals("")) {
            empty_counter++;
        }
        if (output_str != null && output_str.length() > 0
                && sql.indexOf("order by") <= 0) {
            output_str = "- output_ordered: [" + output_str + "]"
                    + System.getProperty("line.separator");
        } else {
            output_str = "- output: [" + output_str + "]"
                    + System.getProperty("line.separator");
        }

        return output_str;
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
        return new File(path + "test-" + targetArea + "-" + modifier + ".yaml")
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
            System.out.println("MySQL(sql): " + sql);
            System.out.println("MySQL(returns): " + retVal);
            System.out.println("MySQL ERROR:  " + e.getMessage());
            //System.exit(-1);  // when you want a hard break
            System.out.println("");
        }
        return retVal;
    }

    /* Holds information about joins to determine what tables join in what fashion 
     * 
     * TODO: hook up alias feature for when same table is both parent and child  
     * */
    public static class Relationship {

        public Relationship(String primaryTable, String secondaryTable,
                String primaryKey, String secondaryKey) {
            super(); // expected to be passed as this order
            this.primaryTable = primaryTable; // %1$s
            this.secondaryTable = secondaryTable; // %2$s
            this.primaryKey = primaryKey; // %3$s
            this.secondaryKey = secondaryKey; // %4$s
            //this.primaryTable_alias = ;
            //this.secondaryTable_alias = ;
        }

        public String primaryTable;
        //public String primaryTable_alias;
        public String secondaryTable;
        //public String secondaryTable_alias;
        public String primaryKey;
        public String secondaryKey;
    }
}
