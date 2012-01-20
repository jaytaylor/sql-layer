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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;

/*
 * creates tests for bitwise math operations
 * */
public class AllQueryComboCreator extends GenericCreator implements Runnable {

    public static final String STR_METHOD = "[s]=";
    public static final String DT_METHOD = "[d]=";
    public static final String INT_METHOD = "[i]=";
    static final String TARGET_AREA = "query-combo";
    StringBuilder yaml_file_contents = null;
    String server = "localhost";
    String username = "root";
    String password = "";
    int counter = 0;
    StringBuilder sb = new StringBuilder();
    Formatter formatter = new Formatter(sb, Locale.US);
    public final String[] QUANTIFIERS = { "Distinct", "All", "" };
    public final String[] FUNCTION_LIST = { "[s]=%1$s = CONCAT('%2$s','%3$s')",
            "[s]=%1$s like ('%2$s')" };
    public final String[] AG_FUNCTION_LIST = { "SUM", "AVG", "COUNT", "MIN",
            "MAX", "COUNT(*)" };
    //    sum([all|distinct] expression)
    //
    //    Total of the (distinct) values in the expression
    //
    //    avg([all|distinct] expression)
    //
    //    Average of the (distinct) values in the expression
    //
    //    count([all|distinct] expression)
    //
    //    Number of (distinct) non-null values in the expression
    //
    //    count(*)
    //
    //    Number of selected rows
    //
    //    max(expression)
    //
    //    Highest value in the expression
    //
    //    min(expression)
    //
    //    Lowest value in the expression
    String path = System.getProperty("user.dir")
            + "/src/test/resources/com/akiban/sql/pg/yaml/functional/";

    /**
     * @param args
     */
    public static void main(String[] args) {
        AllQueryComboCreator b = new AllQueryComboCreator();
        b.setup();
        b.run();
    }

    public final String[] INT_FIELDS = { "test.customers.customer_id",
            "test.addresses.customer_id", "test.orders.customer_id",
            "test.orders.order_id" };
    public final String[] STR_FIELDS = { "test.customers.customer_title",
            "test.addresses.state", "test.states.name", "test.books.book_name",
            "test.customers.customer_name" }; //, "customers.primary_payment_code", "customers.payment_status", "orders.ship_priority" };
    public final String[] DT_FIELDS = { "test.orders.order_date",
            "test.orders.ship_date" };
    public final String[] TABLES = { "test.customers", "test.orders",
            "test.addresses", "test.books", "test.states", "test.items" };
    final Relationship[] RELATIONSHIPS = {
            new Relationship("test.customers", "test.orders", "customer_id",
                    "customer_id"),
            new Relationship("test.customers", "test.addresses", "customer_id",
                    "customer_id"),
            new Relationship("test.orders", "test.items", "order_id",
                    "order_id"),
            new Relationship("test.books", "test.items", "book_id", "book_id"),
            new Relationship("test.states", "test.addresses", "code", "state") };

    public final String[] INT_PARAMS = { "-5", "0", "2", "99" };
    public final String[] STR_PARAMS = { "ABCD", "FONDUE", "CCCCC", "ABDFTERE" };
    public final String[] DT_PARAMS = { "1999-03-04", "2012-12-31" };

    //  SELECT [ <set quantifier> ] <select list> <table expression>
    //    <table expression>    ::= 
    //            <from clause>
    //            [ <where clause> ]
    //            [ <group by clause> ]
    //            [ <having clause> ]
    // order by
    // limit 

    // subquery

    // complex joins (number, conditions)

    public void setup() {

    }

    @Override
    public void run() {
        HashSet<String> string_array;
        HashSet<HashSet<String>> select_fields;
        BufferedWriter master_writer;
        // only needed when changing data loads
        //loadData();

        yaml_file_contents = new StringBuilder(header());

        string_array = new HashSet<String>();

        string_array.addAll(new ArrayList<String>(Arrays.asList(STR_FIELDS)));
        string_array.addAll(new ArrayList<String>(Arrays.asList(INT_FIELDS)));
        string_array.addAll(new ArrayList<String>(Arrays.asList(DT_FIELDS)));

        long time = System.currentTimeMillis();
        System.out.println(string_array.size());
        select_fields = allCombos(string_array);
        System.out.println(eol + "" + (System.currentTimeMillis() - time)
                + " ms");
        System.out.println(select_fields.size());
        
        ArrayList<SQLStatement> stmt_list =  new ArrayList<AllQueryComboCreator.SQLStatement>();
        Iterator<HashSet<String>> i = select_fields.iterator();
        while (i.hasNext()) {
            SQLStatement temp = new SQLStatement();
            
            temp.setRoot_string_array(new HashSet<String>(i.next()));
            temp.setFields(turnArrayToCommaDelimtedList(temp.getRoot_string_array()));
            temp.setTableList(temp.getRoot_string_array().toString());
            if (!stmt_list.contains(temp)) {
                stmt_list.add(temp);
            } else {
                System.out.println("dup");
            }
        }

        //        master_writer = getAppender("select");
        //        try {
        //            try {
        //                justSelectIterations(master_writer, select_fields);
        //            } catch (IOException e) {
        //                System.out.println(e.getMessage());
        //            }
        //        } finally {
        //            try {
        //                master_writer.close();
        //            } catch (IOException e) {
        //                System.out.println(e.getMessage());
        //            }
        //        }

        //        master_writer = getAppender("where");
        //        try {
        //            justWhereIterations_String(master_writer, new HashSet<String>(
        //                    Arrays.asList(STR_FIELDS)));
        //            master_writer.flush();
        //            justWhereIterations_Integer(master_writer, new HashSet<String>(
        //                    Arrays.asList(INT_FIELDS)));
        //            master_writer.flush();
        //            justWhereIterations_DateTime(master_writer, new HashSet<String>(
        //                    Arrays.asList(DT_FIELDS)));
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        } finally {
        //            try {
        //                master_writer.close();
        //            } catch (IOException e) {
        //                System.out.println(e.getMessage());
        //            }
        //        }
        //
        //        master_writer = getAppender("join");
        //        try {
        //            justJoinIterations(master_writer, string_array);
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        } finally {
        //            try {
        //                master_writer.close();
        //            } catch (IOException e) {
        //                System.out.println(e.getMessage());
        //            }
        //        }
        //
        //        master_writer = getAppender("orderby");
        //        try {
        //            justOrderby(master_writer, select_fields);
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        } finally {
        //            try {
        //                master_writer.close();
        //            } catch (IOException e) {
        //                System.out.println(e.getMessage());
        //            }
        //        }

//        master_writer = getAppender("limit");
//        try {
//            justLimitIterations(master_writer, string_array);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                master_writer.close();
//            } catch (IOException e) {
//                System.out.println(e.getMessage());
//            }
//        }
        
      master_writer = getAppender("test1");
      try {
          for (int x=0;x < stmt_list.size();x++) {
              //for (int a=1;a < stmt_list.get(x).getRoot_string_array().size();a++) {
                  
                  stmt_list.set(x, justWhereIterations_DateTime(stmt_list.get(x), 1));
                  writeYamlBlock(master_writer, stmt_list.get(x).getSQL());
              //}
          }
      } catch (IOException e) {
          System.out.println("Error(test1):"+e.getMessage());
      } finally {
          try {
              master_writer.close();
          } catch (IOException e) {
              System.out.println("Error(test1):"+e.getMessage());
          }
      }
        
        
        
        System.out.println("Done!");
    }

    private BufferedWriter getAppender(String modifier) {
        OutputStream os = null;
        try {
            os = new FileOutputStream(path + "test-" + TARGET_AREA + "-"
                    + modifier + ".yaml");
        } catch (FileNotFoundException e) {
            System.out.println("Error(getAppender):"+e.getMessage());
        }
        Writer writer = null;
        try {
            writer = new OutputStreamWriter(os, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.out.println("Error(getAppender):"+e.getMessage());
        }
        return new BufferedWriter(writer);
    }

    private void justWhereIterations_DateTime(BufferedWriter writer,
            HashSet<String> string_array) throws IOException {
        SQLStatement stmt = new SQLStatement();
        stmt.setFields(turnArrayToCommaDelimtedList(string_array));
        stmt.setTableList(string_array.toString());

        Iterator<String> i = string_array.iterator();
        while (i.hasNext()) {
            String field = i.next();

            for (int a = 0; a < FUNCTION_LIST.length; a++) {
                if (FUNCTION_LIST[a].startsWith(DT_METHOD)) {
                    for (int x = 0; x < DT_PARAMS.length; x++) {
                        stmt.setWhere(format(x, a, field, DT_PARAMS));
                        writeYamlBlock(writer, stmt.getSQL());
                    }
                }
            }
        }

    }

    private SQLStatement justWhereIterations_DateTime(SQLStatement stmt, int iterations)
            throws IOException {
        Iterator<String> i = stmt.root_string_array.iterator();
        while (i.hasNext() && iterations-- >= 0) {
            String field = i.next();
            for (int a = 0; a < FUNCTION_LIST.length; a++) {
                if (FUNCTION_LIST[a].startsWith(DT_METHOD)) {
                    for (int x = 0; x < DT_PARAMS.length; x++) {
                        stmt.setWhere(format(x, a, field, DT_PARAMS));
                    }
                }
            }
        }
        return stmt;

    }

    private void justWhereIterations_Integer(BufferedWriter writer,
            HashSet<String> p_string_array) throws IOException {
        SQLStatement stmt = new SQLStatement();
        stmt.setFields(turnArrayToCommaDelimtedList(p_string_array));
        stmt.setTableList(p_string_array.toString());

        Iterator<String> i = p_string_array.iterator();
        while (i.hasNext()) {
            String field = i.next();

            for (int a = 0; a < FUNCTION_LIST.length; a++) {
                if (FUNCTION_LIST[a].startsWith(INT_METHOD)) {
                    for (int x = 0; x < INT_PARAMS.length; x++) {
                        stmt.setWhere(format(x, a, field, INT_PARAMS));
                        writeYamlBlock(writer, stmt.getSQL());
                    }
                }
            }
        }

    }

    public static final String[] ORDER_BY_DIRECTION = { " ASC ", " DESC " };

    @SuppressWarnings("unchecked")
    private void justOrderby(BufferedWriter writer,
            HashSet<HashSet<String>> p_string_array) throws IOException {
        SQLStatement stmt = new SQLStatement();
        ArrayList<String> temp_row;
        ArrayList<String> temp_row2;
        Iterator<HashSet<String>> i = p_string_array.iterator();
        while (i.hasNext()) {
            HashSet<String> row = (HashSet<String>) i.next();
            stmt.setFields(turnArrayToCommaDelimtedList(row));
            stmt.setTableList(row.toString());

            for (int x = 0; x < row.size(); x++) {
                for (int b = 0; b < row.size(); b++) {
                    temp_row = new ArrayList<String>(row);
                    Collections.swap(temp_row, x, b);
                    for (int a = 0; a < ORDER_BY_DIRECTION.length; a++) {
                        temp_row2 = new ArrayList<String>(temp_row);
                        temp_row2.set(x, temp_row2.get(x)
                                + ORDER_BY_DIRECTION[a]);
                        stmt.setOrderby(turnArrayToCommaDelimtedList(temp_row2));
                        writeYamlBlock(writer, stmt.getSQL());
                    }
                }
            }
        }
    }

    public static final String[] JOIN_OTHER = { " CROSS JOIN %3$s " }; // " (%3$s) "

    public static final String[] JOIN_NATURAL = { " NATURAL ", " " };
    public static final String[] JOIN_TYPE = { " INNER ", " LEFT ", " RIGHT ",
            " LEFT OUTER ", " RIGHT OUTER ", " " }; // MySQL does not support " FULL ", " FULL OUTER ", " UNION ", 
    public static final String[] JOIN_SPEC = { " ON %1$s.%2$s = %3$s.%4$s ",
            " USING (%4$s) " }; // unconditional join not supported in MySQL , "" };
    public static final String JOIN = " JOIN %3$s ";

    private void justJoinIterations(BufferedWriter writer,
            HashSet<String> string_array) throws IOException {
        SQLStatement stmt = new SQLStatement();
        stmt.setFields(turnArrayToCommaDelimtedList(string_array));

        for (int x = 0; x < RELATIONSHIPS.length; x++) {
            stmt.setTableList(string_array.toString());
            if (stmt.getTableList().contains(
                    RELATIONSHIPS[x].primaryTable.toUpperCase())
                    && stmt.getTableList().contains(
                            RELATIONSHIPS[x].secondaryTable.toUpperCase())) {
                stmt.getTableList().remove(
                        RELATIONSHIPS[x].secondaryTable.toUpperCase());
                for (int a = 0; a < JOIN_NATURAL.length; a++) {
                    for (int b = 0; b < JOIN_TYPE.length; b++) {
                        for (int c = 0; c < JOIN_SPEC.length; c++) {
                            if (JOIN_NATURAL[a].equalsIgnoreCase(" NATURAL ")
                                    && JOIN_TYPE[a].equalsIgnoreCase(" INNER ")) {
                                continue; // MySQL does not support this case
                            }
                            formatter.format(JOIN_NATURAL[a] + JOIN_TYPE[b]
                                    + JOIN + JOIN_SPEC[c],
                                    RELATIONSHIPS[x].primaryTable,
                                    RELATIONSHIPS[x].primaryKey,
                                    RELATIONSHIPS[x].secondaryTable,
                                    RELATIONSHIPS[x].secondaryKey);
                            stmt.joins = sb.toString();
                            writeYamlBlock(writer, stmt.getSQL());
                            sb.setLength(0);
                        }
                    }
                }
                for (int a = 0; a < JOIN_OTHER.length; a++) {
                    formatter.format(JOIN_OTHER[a],
                            RELATIONSHIPS[x].primaryTable,
                            RELATIONSHIPS[x].primaryKey,
                            RELATIONSHIPS[x].secondaryTable,
                            RELATIONSHIPS[x].secondaryKey);
                    stmt.joins = sb.toString();
                    writeYamlBlock(writer, stmt.getSQL());
                    sb.setLength(0);
                }

            }
        }

    }

    private void justWhereIterations_String(BufferedWriter writer,
            HashSet<String> string_array) throws IOException {
        SQLStatement stmt = new SQLStatement();
        stmt.setFields(turnArrayToCommaDelimtedList(string_array));
        stmt.setTableList(string_array.toString());

        Iterator<String> i = string_array.iterator();
        while (i.hasNext()) {
            String field = i.next();

            for (int a = 0; a < FUNCTION_LIST.length; a++) {
                if (FUNCTION_LIST[a].startsWith(STR_METHOD)) {
                    for (int x = 0; x < STR_PARAMS.length; x++) {
                        stmt.setWhere(format(x, a, field, STR_PARAMS));
                        writeYamlBlock(writer, stmt.getSQL());
                    }
                }
            }
        }
    }

    private void justLimitIterations(BufferedWriter writer,
            HashSet<String> string_array) throws IOException {
        SQLStatement stmt = new SQLStatement();
        stmt.setFields(turnArrayToCommaDelimtedList(string_array));
        stmt.setTableList(string_array.toString());

        Iterator<String> i = string_array.iterator();
        while (i.hasNext()) {
            String field = i.next();
            stmt.setLimit(1);
            writeYamlBlock(writer, stmt.getSQL());
        }
    }

    protected String format(int x, int a, String field, String[] source) {
        formatter.format(new String(FUNCTION_LIST[a]).substring(4), field,
                source[x], source[Math.min(source.length - 1, (x + 1))],
                source[Math.min(source.length - 1, (x + 2))],
                source[Math.min(source.length - 1, (x + 3))],
                source[Math.min(source.length - 1, (x + 4))]);
        String retVal = sb.toString();
        sb.setLength(0);
        return retVal;
    }

    private String turnArrayToCommaDelimtedList(HashSet<String> string_array) {
        return turnArrayToCommaDelimtedList(new ArrayList<String>(string_array));
    }

    private void justSelectIterations(BufferedWriter writer,
            HashSet<HashSet<String>> select_fields) throws IOException {
        SQLStatement stmt = new SQLStatement();
        Iterator<HashSet<String>> i = select_fields.iterator();
        while (i.hasNext()) {
            ArrayList<String> next = new ArrayList<String>(i.next());

            stmt.setTableList(next.toString());
            stmt.setFields(turnArrayToCommaDelimtedList(next));
            // select A,B,C
            writeYamlBlock(writer, stmt.getSQL());

            processEachColumn(writer, stmt, next);

            writer.flush();
        }
    }

    public void processEachColumn(BufferedWriter writer, SQLStatement stmt,
            ArrayList<String> next) throws IOException {

        // all combinations of each string, changing order
        for (int b = 0; b < next.size(); b++) {
            ArrayList<String> select_fields = new ArrayList<String>(next);
            Collections.swap(select_fields, 0, b);
            for (int c = 1; c < next.size(); c++) {
                if (c == b) {
                    continue;
                }
                ArrayList<String> temp_select_fields = new ArrayList<String>(
                        select_fields);
                Collections.swap(temp_select_fields, c, b);
                stmt.setGroupby(turnArrayToCommaDelimtedList(temp_select_fields));

                temp_select_fields.set(0,
                        " distinct " + temp_select_fields.get(b));
                stmt.setFields(turnArrayToCommaDelimtedList(temp_select_fields));
                writeYamlBlock(writer, stmt.getSQL());

                temp_select_fields = new ArrayList<String>(select_fields);
                temp_select_fields.set(0, " all " + temp_select_fields.get(b));
                stmt.setFields(turnArrayToCommaDelimtedList(temp_select_fields));
                writeYamlBlock(writer, stmt.getSQL());

                stmt.setGroupby("");
            }
        }
    }

    protected ArrayList<String> extractTableList(String sql) {
        ArrayList<String> retVal = new ArrayList<String>();
        sql = sql.replace("[", "");
        String[] data = sql.split("[,]");
        if (data.length > 0 && sql.trim().length() > 0) {
            for (int x = 0; x < data.length; x++) {
                String table = data[x].substring(0, data[x].lastIndexOf("."))
                        .toUpperCase().trim();
                if (!retVal.contains(table)) {
                    retVal.add(table);
                }
            }
        }
        return retVal;
    }

    private void writeYamlBlock(BufferedWriter writer, String sql)
            throws IOException {
        writer.append("---" + eol);
        writer.append("- Statement: " + sql + eol);
        String mySQL_sql = generateMySQL(sql);
        String expected_output = callMySQL(mySQL_sql);
        writer.append(expected_output + eol);
        //System.out.println(sql);
        counter++;
    }

    private String callMySQL(String sql) {
        String retVal = null;

        try {
            retVal = generateOutputFromInno(server, username, password, sql,
                    null);
            //System.out.print(".");
        } catch (Exception e) {
            System.out.println("");
            System.out.println("MySQL: " + sql);
            System.out.println("MySQL: " + retVal);
            System.out.println("MySQL ERROR:  " + e.getMessage());
            System.exit(-1);
        }
        return retVal;
    }

    private String generateMySQL(String sql) {
        // replace any syntax that is different between systems
        return sql;
    }

    private void finalizeFile(String modifier) {
        // will drop the files in the same branch as where this code is running
        // sql gets dropped in the root directory of branch

        recordYamlToDisk(path, modifier);
        System.out.println("Test count generated is " + counter);
        counter = 0;
    }

    private String header() {
        return "# generated by com.akiban.sql.test.AllQueryComboCreator on "
                + new Date() + eol + "---" + eol
                + "- Include: all-caoi-schema.yaml" + eol;
    }

    private void recordYamlToDisk(String path, String modifier) {
        try {
            save(path + "test-" + TARGET_AREA + "-" + modifier + ".yaml",
                    yaml_file_contents);
            System.out.println(eol + "contents saved to " + path + "test-"
                    + TARGET_AREA + "-" + modifier + ".yaml");
            yaml_file_contents = new StringBuilder(header());
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    // all combinations of strings (not considering order)
    public HashSet<HashSet<String>> allCombos(HashSet<String> string_array) {
        HashSet<HashSet<String>> retVal = new HashSet<HashSet<String>>();
        retVal.add(string_array);
        for (int a = 0; a < string_array.size(); a += 3) {
            HashSet<String> temp = new HashSet<String>(string_array);
            temp.remove(temp.toArray()[a]);

            if (temp.size() > 0) {

                HashSet<HashSet<String>> list = allCombos(temp);
                retVal.addAll(list);
            }
        }
        //System.out.println("*");
        return retVal;
    }

    private class Relationship {

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

    private class SQLStatement {
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result
                    + ((fields == null) ? 0 : fields.hashCode());
            result = prime * result + ((from == null) ? 0 : from.hashCode());
            result = prime * result
                    + ((groupby == null) ? 0 : groupby.hashCode());
            result = prime * result
                    + ((having == null) ? 0 : having.hashCode());
            result = prime * result + ((joins == null) ? 0 : joins.hashCode());
            result = prime * result + ((limit == null) ? 0 : limit.hashCode());
            result = prime * result
                    + ((orderby == null) ? 0 : orderby.hashCode());
            result = prime * result
                    + ((quantifier == null) ? 0 : quantifier.hashCode());
            result = prime
                    * result
                    + ((root_string_array == null) ? 0 : root_string_array
                            .hashCode());
            result = prime * result
                    + ((tableList == null) ? 0 : tableList.hashCode());
            result = prime * result + ((where == null) ? 0 : where.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SQLStatement other = (SQLStatement) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (fields == null) {
                if (other.fields != null)
                    return false;
            } else if (!fields.equals(other.fields))
                return false;
            if (from == null) {
                if (other.from != null)
                    return false;
            } else if (!from.equals(other.from))
                return false;
            if (groupby == null) {
                if (other.groupby != null)
                    return false;
            } else if (!groupby.equals(other.groupby))
                return false;
            if (having == null) {
                if (other.having != null)
                    return false;
            } else if (!having.equals(other.having))
                return false;
            if (joins == null) {
                if (other.joins != null)
                    return false;
            } else if (!joins.equals(other.joins))
                return false;
            if (limit == null) {
                if (other.limit != null)
                    return false;
            } else if (!limit.equals(other.limit))
                return false;
            if (orderby == null) {
                if (other.orderby != null)
                    return false;
            } else if (!orderby.equals(other.orderby))
                return false;
            if (quantifier == null) {
                if (other.quantifier != null)
                    return false;
            } else if (!quantifier.equals(other.quantifier))
                return false;
            if (root_string_array == null) {
                if (other.root_string_array != null)
                    return false;
            } else if (!root_string_array.equals(other.root_string_array))
                return false;
            if (tableList == null) {
                if (other.tableList != null)
                    return false;
            } else if (!tableList.equals(other.tableList))
                return false;
            if (where == null) {
                if (other.where != null)
                    return false;
            } else if (!where.equals(other.where))
                return false;
            return true;
        }

        private HashSet<String> root_string_array;
        public String quantifier = "";
        private String fields = "";
        public String from = "";
        public String joins = "";
        private String where = "";
        private String groupby = "";
        public String having = "";
        private String orderby = "";
        private String limit = "";
        private ArrayList<String> tableList = new ArrayList<String>();

        public String getSQL() {
            if (getTableList().size() > 0) {
                from = " From " + turnArrayToCommaDelimtedList(getTableList());
            } else {
                System.out.println("no tables to select from");
                System.exit(-1);
            }

            return "select " + quantifier + getFields() + from + joins
                    + getWhere() + getGroupby() + having + getOrderby()
                    + getLimit();
        }

        public String getWhere() {
            return where;
        }

        public void setWhere(String where) {
            this.where = " where " + where;
        }

        public void addWhere(String where_fragment) {
            this.where += where_fragment;
        }

        public String getGroupby() {
            return groupby;
        }

        public void setGroupby(String p_groupby) {
            if (p_groupby.length() > 0) {
                this.groupby = " group by " + p_groupby;
            } else {
                this.groupby = "";
            }
        }

        public void addGroupby(String groupby_fragment) {
            this.groupby += groupby_fragment;
        }

        public String getOrderby() {
            return orderby;
        }

        public void setOrderby(String p_orderby) {
            if (p_orderby.length() > 0) {
                this.orderby = " order by " + p_orderby;
            } else {
                this.orderby = "";
            }
        }

        public String getFields() {
            return fields;
        }

        public void setFields(String p_fields) {
            this.fields = p_fields;

        }

        private ArrayList<String> getTableList() {
            return tableList;
        }

        private void setTableList(String p_fields) {
            this.tableList = extractTableList(p_fields);
        }

        public String getLimit() {
            return limit;
        }

        public void setLimit(int p_limit) {
            this.limit = " LIMIT " + p_limit;
        }

        public HashSet<String> getRoot_string_array() {
            return root_string_array;
        }

        public void setRoot_string_array(HashSet<String> p_root_string_array) {
            this.root_string_array = new HashSet<String>();
            this.root_string_array = p_root_string_array;
        }

        private AllQueryComboCreator getOuterType() {
            return AllQueryComboCreator.this;
        }

    }

    //    private ArrayList<String> getColumnsforTable(String table) {
    //        ArrayList<String> retVal = new ArrayList<String>();
    //        for (Iterator<String> i = string_array.iterator(); i.hasNext();) {
    //            String element = i.next();
    //            if (element.startsWith(table)) {
    //                retVal.add(element);
    //            }
    //        }
    //        return retVal;
    //    }

    public void loadData() {
        ArrayList<String> insert_list = new ArrayList<String>();
        ArrayList<String> create_list = new ArrayList<String>();
        ArrayList<String> drop_list = new ArrayList<String>();

        drop_list.add("test.states");
        drop_list.add("test.books");
        drop_list.add("test.items");
        drop_list.add("test.orders");
        drop_list.add("test.addresses");
        drop_list.add("test.customers");

        create_list
                .add("test.states (code char(2) not null, name varchar(50) not null);");
        create_list
                .add("test.books (book_id integer not null primary key generated by default as identity, book_name varchar(50) not null, copyright_year date, author_name varchar(50) not null);");
        create_list
                .add("test.customers (customer_id int not null primary key generated by default as identity, customer_title varchar(255), customer_name varchar(255) not null, primary_payment_code char(1) not null default 'C', payment_status char(4) not null default 'ABCD', comment varchar(255));");
        create_list
                .add("test.addresses (customer_id int not null, state varchar(2) not null, zip_code varchar(5) not null, phone varchar (15), address_type char(1) not null default 'N', primary key (customer_id, zip_code), grouping foreign key (customer_id) references test.customers (customer_id));");
        create_list
                .add("test.orders (order_id int not null primary key generated by default as identity, customer_id int not null, order_date date not null, order_status char (1) not null default 'N', order_priority varchar(15) not null default 'Standard', ship_date date, ship_priority varchar(15), ship_method varchar(15), ship_parts int, update_time timestamp default current_timestamp, instructions varchar(255), grouping foreign key (customer_id) REFERENCES test.customers (customer_id));");
        create_list
                .add("test.items (order_id int not null, book_id int not null, quantity int not null, unit_price int not null, discount float, tax float, item_status char(1) not null default 'N', shipment_part int, color varchar(20), primary key(order_id, book_id), grouping foreign key (order_id) references test.orders (order_id));");

        insert_list
                .add("Insert into test.states (name, code) values ('ALABAMA','AL')");
        insert_list
                .add("Insert into test.states (name,code) values ('ALASKA'  ,'AK')");
        insert_list
                .add("Insert into test.states (name,code) values ('AMERICAN SAMOA'  ,'AS')");
        insert_list
                .add("Insert into test.states (name,code) values ('ARIZONA','AZ')");
        insert_list
                .add("Insert into test.states (name,code) values ('ARKANSAS','AR')");
        insert_list
                .add("Insert into test.states (name,code) values ('CALIFORNIA','CA')");
        insert_list
                .add("Insert into test.states (name,code) values ('COLORADO','CO')");
        insert_list
                .add("Insert into test.states (name,code) values ('CONNECTICUT','CT')");
        insert_list
                .add("Insert into test.states (name,code) values ('DELAWARE','DE')");
        insert_list
                .add("Insert into test.states (name,code) values ('DISTRICT OF COLUMBIA','DC')");
        insert_list
                .add("Insert into test.states (name,code) values ('FEDERATED test.states OF MICRONESIA','FM')");
        insert_list
                .add("Insert into test.states (name,code) values ('FLORIDA','FL')");
        insert_list
                .add("Insert into test.states (name,code) values ('GEORGIA','GA')");
        insert_list
                .add("Insert into test.states (name,code) values ('GUAM GU','GU')");
        insert_list
                .add("Insert into test.states (name,code) values ('HAWAII','HI')");
        insert_list
                .add("Insert into test.states (name,code) values ('IDAHO','ID')");
        insert_list
                .add("Insert into test.states (name,code) values ('ILLINOIS','IL')");
        insert_list
                .add("Insert into test.states (name,code) values ('INDIANA','IN')");
        insert_list
                .add("Insert into test.states (name,code) values ('IOWA','IA')");
        insert_list
                .add("Insert into test.states (name,code) values ('KANSAS','KS')");
        insert_list
                .add("Insert into test.states (name,code) values ('KENTUCKY','KY')");
        insert_list
                .add("Insert into test.states (name,code) values ('LOUISIANA','LA')");
        insert_list
                .add("Insert into test.states (name,code) values ('MAINE','ME')");
        insert_list
                .add("Insert into test.states (name,code) values ('MARSHALL ISLANDS','MH')");
        insert_list
                .add("Insert into test.states (name,code) values ('MARYLAND','MD')");
        insert_list
                .add("Insert into test.states (name,code) values ('MASSACHUSETTS','MA')");
        insert_list
                .add("Insert into test.states (name,code) values ('MICHIGAN','MI')");
        insert_list
                .add("Insert into test.states (name,code) values ('MINNESOTA','MN')");
        insert_list
                .add("Insert into test.states (name,code) values ('MISSISSIPPI','MS')");
        insert_list
                .add("Insert into test.states (name,code) values ('MISSOURI','MO')");
        insert_list
                .add("Insert into test.states (name,code) values ('MONTANA','MT')");
        insert_list
                .add("Insert into test.states (name,code) values ('NEBRASKA','NE')");
        insert_list
                .add("Insert into test.states (name,code) values ('NEVADA','NV')");
        insert_list
                .add("Insert into test.states (name,code) values ('NEW HAMPSHIRE','NH')");
        insert_list
                .add("Insert into test.states (name,code) values ('NEW JERSEY','NJ')");
        insert_list
                .add("Insert into test.states (name,code) values ('NEW MEXICO','NM')");
        insert_list
                .add("Insert into test.states (name,code) values ('NEW YORK','NY')");
        insert_list
                .add("Insert into test.states (name,code) values ('NORTH CAROLINA','NC')");
        insert_list
                .add("Insert into test.states (name,code) values ('NORTH DAKOTA','ND')");
        insert_list
                .add("Insert into test.states (name,code) values ('NORTHERN MARIANA ISLANDS', 'MP')");
        insert_list
                .add("Insert into test.states (name,code) values ('OHIO','OH')");
        insert_list
                .add("Insert into test.states (name,code) values ('OKLAHOMA','OK')");
        insert_list
                .add("Insert into test.states (name,code) values ('OREGON','OR')");
        insert_list
                .add("Insert into test.states (name,code) values ('PALAU','PW')");
        insert_list
                .add("Insert into test.states (name,code) values ('PENNSYLVANIA','PA')");
        insert_list
                .add("Insert into test.states (name,code) values ('PUERTO RICO','PR')");
        insert_list
                .add("Insert into test.states (name,code) values ('RHODE ISLAND','RI')");
        insert_list
                .add("Insert into test.states (name,code) values ('SOUTH CAROLINA','SC')");
        insert_list
                .add("Insert into test.states (name,code) values ('SOUTH DAKOTA','SD')");
        insert_list
                .add("Insert into test.states (name,code) values ('TENNESSEE','TN')");
        insert_list
                .add("Insert into test.states (name,code) values ('TEXAS','TX')");
        insert_list
                .add("Insert into test.states (name,code) values ('UTAH','UT')");
        insert_list
                .add("Insert into test.states (name,code) values ('VERMONT','VT')");
        insert_list
                .add("Insert into test.states (name,code) values ('VIRGIN ISLANDS','VI')");
        insert_list
                .add("Insert into test.states (name,code) values ('VIRGINIA','VA')");
        insert_list
                .add("Insert into test.states (name,code) values ('WASHINGTON','WA')");
        insert_list
                .add("Insert into test.states (name,code) values ('WEST VIRGINIA','WV')");
        insert_list
                .add("Insert into test.states (name,code) values ('WISCONSIN','WI')");
        insert_list
                .add("Insert into test.states (name,code) values ('WYOMING','WY')");
        insert_list
                .add("Insert into test.states (name,code) values ('Armed Forces Africa','AE')");
        insert_list
                .add("Insert into test.states (name,code) values ('Armed Forces America','AA')");
        insert_list
                .add("Insert into test.states (name,code) values ('Armed Forces Pacific','AP')");

        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (1,'A Prayer for Owen Meany','John Irving ','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (2,'A Suitable Boy','Vikram Seth','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (3,'American Pastoral','Philip Roth','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (4,'Atonement','Ian McEwan ','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (5,'Being Dead','Jim Crace','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (6,'Birdsong','Sebastian Faulks','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (7,'Captain Corelli''s Mandolin','Louis de Bernieres','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (8,'Cloudstreet','Tim Winton','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (9,'Disgrace','JM Coetzee','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (10,'Enduring Love','Ian McEwan','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (11,'Faith Singer','Rosie Scott','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (12,'Fingersmith','Sarah Waters','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (13,'Fred and Edie','Jill Dawson ','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (14,'Fugitive Pieces','Anne Michaels','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (15,'Girl with a Pearl Earring','Tracy Chevalier','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (16,'Grace Notes','Bernard MacLaverty','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (17,'High Fidelity','Nick Hornby','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (18,'His Dark Materials Trilogy','Philip Pullman','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (19,'Hotel World','Ali Smith','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (20,'Middlesex','Jeffrey Eugenides','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (21,'Midnight''s Children','Salman Rushdie','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (22,'Misery','Stephen King','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (23,'Miss Smilla''s Feeling for Snow','Peter Hoeg','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (24,'Money','Martin Amis','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (25,'Music and Silence','Rose Tremain','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (26,'One Hundred Years of Solitude','Gabriel Garcia Marquez','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (27,'Oranges Are Not The Only Fruit','Jeanette Winterson','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (28,'Riders','Jilly Cooper','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (29,'Slaughterhouse-five','Kurt Vonnegut','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (30,'The Blind Assassin','Margaret Atwood','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (31,'The Corrections','Jonathan Franzen','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (32,'The Golden Notebook','Doris Lessing','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (33,'The Handmaid''s Tale','Margaret Atwood','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (34,'The House of Spirits','Isabelle Allende','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (35,'The Name of the Rose','Umberto Eco','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (36,'The Passion','Jeanette Winterson','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (37,'The Poisonwood Bible','Barbara Kingsolver','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (38,'The Rabbit test.books','John Updike','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (39,'The Regeneration Trilogy','Pat Barker','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (40,'The Secret History','Donna Tartt','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (41,'The Shipping News','E Annie Proulx','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (42,'The Tin Drum','Gunter Grass','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (43,'The Wind Up Bird Chronicle','Haruki Murakami','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (44,'The Women''s Room','Marilyn French','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (45,'Tracey Beaker','Jacqueline Wilson','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (46,'Trainspotting','Irvine Welsh','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (47,'Unless','Carol Shields','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (48,'What a Carve Up!','Jonathan Coe','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (49,'What I Loved','Siri Hustvedt','2000-01-01');");
        insert_list
                .add("Insert into test.books (book_id, book_name , author_name, copyright_year) values (50,'White Teeth','Zadie Smith','2000-01-01')");
        insert_list
                .add("Insert into test.customers (customer_id, customer_title, customer_name, comment) values (1,'Mr.','Bob Jones','No Comment');");
        insert_list
                .add("Insert into test.customers (customer_id, customer_title, customer_name, comment) values (2,'Mrs.','Jenna Jones','No Comment');");
        insert_list
                .add("Insert into test.customers (customer_id, customer_title, customer_name, comment) values (3,'Mr.','Harry Jones','1 small Comment')");
        insert_list
                .add("Insert into test.customers (customer_id, customer_title, customer_name, comment) values (4,'Mrs.','Wendy Wendelton','2 small Comments')");
        insert_list
                .add("Insert into test.addresses (customer_id, state, zip_code, phone) values (1, 'MA', 12345, '781-555-1212'), (2, 'RI', 12335, '781-515-1212'), (3, 'NY', 02345, '315-555-1212')");
        insert_list
                .add("Insert into test.addresses (customer_id, state, zip_code, phone) values (4, 'MA', 12745, '781-151-1212') ");
        insert_list
                .add("Insert into test.orders (order_id, customer_id, order_date, ship_date, ship_priority , ship_method , ship_parts, instructions) values (1,1,'2011-01-02','2011-01-15','A','B',1,'Big instructions')");
        insert_list
                .add("Insert into test.items (order_id, book_id, quantity, unit_price, discount , tax , shipment_part, color) values (1,1,1,1.50,.21,.05,1,'Blue');");
        insert_list
                .add("Insert into test.items (order_id, book_id, quantity, unit_price, discount , tax , shipment_part, color) values (1,2,1,1.50,.21,.05,1,'Blue');");
        insert_list
                .add("Insert into test.items (order_id, book_id, quantity, unit_price, discount , tax , shipment_part, color) values (1,3,1,1.50,.21,.05,1,'Blue');");

        insert_list
                .add("Insert into test.orders (order_id, customer_id, order_date, ship_date, ship_priority , ship_method , ship_parts, instructions) values (2,4,'2011-01-05','2011-01-18','A','B',1,'Big instructions')");
        insert_list
                .add("Insert into test.items (order_id, book_id, quantity, unit_price, discount , tax , shipment_part, color) values (2,10,1,1.50,.21,.05,1,'Blue');");
        insert_list
                .add("Insert into test.items (order_id, book_id, quantity, unit_price, discount , tax , shipment_part, color) values (2,1,1,1.50,.21,.05,1,'Red');");
        insert_list
                .add("Insert into test.orders (order_id, customer_id, order_date, ship_date, ship_priority , ship_method , ship_parts, instructions) values (3,4,'2011-01-22','2011-02-15','A','B',1,'Big instructions')");
        insert_list
                .add("Insert into test.items (order_id, book_id, quantity, unit_price, discount , tax , shipment_part, color) values (3,10,1,1.50,.21,.05,1,'Blue');");
        insert_list
                .add("Insert into test.items (order_id, book_id, quantity, unit_price, discount , tax , shipment_part, color) values (3,1,1,1.50,.21,.05,1,'Red');");

        String path = System.getProperty("user.dir")
                + "/src/test/resources/com/akiban/sql/pg/yaml/functional/";
        StringBuilder sb = new StringBuilder();
        String mysql_sql = null;
        try {
            generateOutputFromInno(server, username, password, "use test;",
                    null);
        } catch (Exception e1) {
            System.out.println("Error(generateOutputFromInno):"+e1.getMessage());
        }
        //sb.append("---" + eol + "- Statement: Use test;" + eol);
        for (int x = 0; x < drop_list.size(); x++) {
            try {
                mysql_sql = "Drop table " + drop_list.get(x) + ";";
                generateOutputFromInno(server, username, password, mysql_sql,
                        null);
                //sb.append("---" + eol + "- DropTable: " + drop_list.get(x)
                //      + eol);
            } catch (Exception e) {
                System.out.println(mysql_sql);
                System.out.println("Error(generateOutputFromInno):"+e.getMessage());
            }
        }

        for (int x = 0; x < create_list.size(); x++) {
            try {
                mysql_sql = create_list.get(x).replace(
                        "generated by default as identity", "");
                mysql_sql = mysql_sql.replace("grouping foreign key",
                        "foreign key");
                mysql_sql = "Create table " + mysql_sql;
                generateOutputFromInno(server, username, password, mysql_sql,
                        null);
            } catch (Exception e) {
                System.out.println(mysql_sql);
                System.out.println("Error(generateOutputFromInno):"+e.getMessage());
            }

            sb.append("---" + eol + "- CreateTable: " + create_list.get(x)
                    + eol);
        }

        for (int x = 0; x < insert_list.size(); x++) {

            try {
                generateOutputFromInno(server, username, password,
                        insert_list.get(x), null);
            } catch (Exception e) {
                System.out.println(insert_list.get(x));
                System.out.println("Error(generateOutputFromInno):"+e.getMessage());
            }
            sb.append("---" + eol + "- Statement: " + insert_list.get(x) + eol);

        }
        sb.append("..." + eol);
        try {
            save(path + "all-caoi-schema.yaml", sb);
        } catch (IOException e) {
            System.out.println("Error(save):"+e.getMessage());
        }
        System.out.println("Load finished");
    }

}
