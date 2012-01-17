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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    StringBuilder yaml_file_contents = new StringBuilder();
    String server = "localhost";
    String username = "root";
    String password = "";
    int counter = 0;
    StringBuilder sb = new StringBuilder();
    Formatter formatter = new Formatter(sb, Locale.US);

    /**
     * @param args
     */
    public static void main(String[] args) {
        AllQueryComboCreator b = new AllQueryComboCreator();
        b.run();
    }

    public final String[] QUANTIFIERS = { "Distinct", "All", "" };
    public final String[] INT_FIELDS = { "customers.customer_id" };
    public final String[] STR_FIELDS = { "customers.customer_title",
            "customers.customer_name", "customers.primary_payment_code",
            "customers.payment_status", "orders.ship_priority" };
    public final String[] DT_FIELDS = { "orders.order_date", "orders.ship_date" };
    public final String[] TABLES = { "customers", "orders" };
    public final String[] INT_PARAMS = { "-5", "0", "2", "99" };
    public final String[] STR_PARAMS = { "ABCD", "FONDUE", "CCCCC", "ABDFTERE" };
    public final String[] DT_PARAMS = { "1999-03-04", "2012-12-31" };
    public final String[] FUNCTION_LIST = { "[i]=%3$s = %1$s MOD %2$s",
            "[s]=%1$s = CONCAT('%2$s','%3$s')",
            "[d]=DATE_ADD(%1$s, INTERVAL %2$s HOUR)" };
    final Relationship[] RELATIONSHIPS = {
            new Relationship("customers", "orders", "customer_id",
                    "customer_id"),
            new Relationship("customers", "addresses", "customer_id",
                    "customer_id") };

    //  SELECT [ <set quantifier> ] <select list> <table expression>
    //    <table expression>    ::= 
    //            <from clause>
    //            [ <where clause> ]
    //            [ <group by clause> ]
    //            [ <having clause> ]

    public void setup() {

    }

    @Override
    public void run() {
        header();

        HashSet<String> string_array = new HashSet<String>(
                Arrays.asList(INT_FIELDS));
        string_array.addAll(new HashSet<String>(Arrays.asList(STR_FIELDS)));
        string_array.addAll(new HashSet<String>(Arrays.asList(DT_FIELDS)));
        HashSet<HashSet<String>> select_fields = allCombos(string_array);

        justSelectIterations(select_fields);

        justWhereIterations(string_array);

        justJoinIterations(string_array);

        finalizeFile();
    }

    private void justJoinIterations(HashSet<String> string_array) {
        SQLStatement stmt = new SQLStatement();
        stmt.fields = turnArrayToCommaDelimtedList(string_array);
        stmt.tableList = extractTableList(string_array.toString());
        for (int x = 0; x < RELATIONSHIPS.length; x++) {
            if (stmt.tableList.contains(RELATIONSHIPS[x].primaryTable)
                    && stmt.tableList.contains(RELATIONSHIPS[x].secondaryTable)) {
                stmt.tableList.remove(RELATIONSHIPS[x].secondaryTable);
                formatter.format(" JOIN %3$s ON %1$s.%2$s = %3$s.%4$s ",
                        RELATIONSHIPS[x].primaryTable,
                        RELATIONSHIPS[x].primaryKey,
                        RELATIONSHIPS[x].secondaryTable,
                        RELATIONSHIPS[x].secondaryKey);
                stmt.joins = sb.toString();
                writeYamlBlock(stmt.getSQL());
                sb.setLength(0);
                formatter.format(" LEFT JOIN %3$s ON %1$s.%2$s = %3$s.%4$s ",
                        RELATIONSHIPS[x].primaryTable,
                        RELATIONSHIPS[x].primaryKey,
                        RELATIONSHIPS[x].secondaryTable,
                        RELATIONSHIPS[x].secondaryKey);
                stmt.joins = sb.toString();
                writeYamlBlock(stmt.getSQL());
                sb.setLength(0);
            }
        }

    }

    private void justWhereIterations(HashSet<String> string_array) {
        SQLStatement stmt = new SQLStatement();
        stmt.fields = turnArrayToCommaDelimtedList(string_array);
        stmt.tableList = extractTableList(string_array.toString());

        Iterator<String> i = string_array.iterator();
        while (i.hasNext()) {
            String field = i.next();

            for (int a = 0; a < FUNCTION_LIST.length; a++) {
                if (FUNCTION_LIST[a].startsWith(STR_METHOD)) {
                    for (int x = 0; x < STR_PARAMS.length; x++) {
                        stmt.setWhere(format(x, a, field, STR_PARAMS));
                        writeYamlBlock(stmt.getSQL());
                    }
                }
                if (FUNCTION_LIST[a].startsWith(INT_METHOD)) {
                    for (int x = 0; x < INT_PARAMS.length; x++) {
                        stmt.setWhere(format(x, a, field, INT_PARAMS));
                        writeYamlBlock(stmt.getSQL());
                    }
                }
                if (FUNCTION_LIST[a].startsWith(DT_METHOD)) {
                    for (int x = 0; x < DT_PARAMS.length; x++) {
                        stmt.setWhere(format(x, a, field, DT_PARAMS));
                        writeYamlBlock(stmt.getSQL());
                    }
                }
            }
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

    private void justSelectIterations(HashSet<HashSet<String>> select_fields) {
        SQLStatement stmt = new SQLStatement();
        Iterator<HashSet<String>> i = select_fields.iterator();
        while (i.hasNext()) {
            ArrayList<String> next = new ArrayList<String>(i.next());

            stmt.tableList = extractTableList(next.toString());
            stmt.fields = turnArrayToCommaDelimtedList(next);
            writeYamlBlock(stmt.getSQL());

            for (int a = 0; a < QUANTIFIERS.length; a++) {
                stmt.quantifier = " " + QUANTIFIERS[a] + " ";

                for (int b = 0; b < next.size(); b++) {
                    @SuppressWarnings("unchecked")
                    ArrayList<String> temp_select_fields = (ArrayList<String>) next
                            .clone();

                    if (QUANTIFIERS[a].length() > 0) {
                        Collections.swap(next, 0, b);
                        temp_select_fields = (ArrayList<String>) next.clone();
                        temp_select_fields.set(0, temp_select_fields.get(b));
                        stmt.setGroupby(temp_select_fields.get(b));

                        stmt.fields = turnArrayToCommaDelimtedList(temp_select_fields);

                        writeYamlBlock(stmt.getSQL());

                        temp_select_fields = (ArrayList<String>) next.clone();
                        stmt.quantifier = "";

                        temp_select_fields.set(0, " count("
                                + temp_select_fields.get(b) + ") ");

                        stmt.fields = turnArrayToCommaDelimtedList(temp_select_fields);

                        writeYamlBlock(stmt.getSQL());

                        temp_select_fields = (ArrayList<String>) next.clone();
                        temp_select_fields.set(0, " count(distinct "
                                + temp_select_fields.get(b) + ") ");

                        stmt.fields = turnArrayToCommaDelimtedList(temp_select_fields);

                        writeYamlBlock(stmt.getSQL());
                        stmt.setGroupby("");
                    }
                }
            }
        }
    }

    protected ArrayList<String> extractTableList(String sql) {
        ArrayList<String> retVal = new ArrayList<String>();
        if (sql.length() > 2) {
            sql = sql.replace("[", " ");
            if (sql.contains(",")) {
                String[] temp = sql.split(",");
                for (int x = 0; x < temp.length; x++) {
                    String[] temp2 = temp[x].split("[.]");
                    for (int y = 0; y < temp2.length; y += 2) {
                        String temp_str = temp2[y].trim();
                        if (temp2[y].indexOf(" ") > 0) {
                            temp_str = temp2[y]
                                    .substring(temp2[y].indexOf(" "));
                        }
                        if (!retVal.contains(temp_str)) {
                            retVal.add(temp_str.trim());
                        }
                    }
                }
            } else {
                String[] temp2 = sql.split("[.]");
                for (int y = 0; y < temp2.length; y += 2) {
                    String temp_str = temp2[y];

                    if (temp2[y].indexOf(" ") > 0) {
                        temp_str = temp2[y].substring(temp2[y].indexOf(" "));
                    }
                    if (!retVal.contains(temp_str)) {
                        retVal.add(temp_str.trim());
                    }
                }
            }
        }

        return retVal;
    }

    private void writeYamlBlock(String sql) {
        yaml_file_contents.append("---" + eol);
        yaml_file_contents.append("- Statement: " + sql + eol);
        String mySQL_sql = generateMySQL(sql);
        String expected_output = callMySQL(mySQL_sql);
        yaml_file_contents.append(expected_output + eol);
        counter++;
    }

    private String callMySQL(String sql) {
        String retVal = "- output: ";
        try {
            retVal = generateOutputFromInno(server, username, password, sql,
                    null);
            System.out.print(".");
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

    private void finalizeFile() {
        // will drop the files in the same branch as where this code is running
        // sql gets dropped in the root directory of branch
        String path = System.getProperty("user.dir") + "/src/test/resources/com/akiban/sql/pg/yaml/functional/";
        recordYamlToDisk(path);
        System.out.println(yaml_file_contents.toString());
        System.out.println("Test generated is " + counter);
    }

    private void header() {
        yaml_file_contents
                .append("# generated by com.akiban.sql.test.AllQueryComboCreator on "
                        + new Date() + eol);
        yaml_file_contents.append("---" + eol
                + "- Include: all-caoi-schema.yaml" + eol);
    }

    private void recordYamlToDisk(String path) {
        try {
            save(path + "test-" + TARGET_AREA + ".yaml", yaml_file_contents);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    class FieldArrayList extends ArrayList<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String toString() {
            // TODO Auto-generated method stub
            return super.toString();
        }

    }

    @SuppressWarnings("unchecked")
    public HashSet<HashSet<String>> allCombos(HashSet<String> string_array) {
        HashSet<HashSet<String>> retVal = new HashSet<HashSet<String>>();
        retVal.add(string_array);
        for (int a = 0; a < string_array.size(); a++) {
            HashSet<String> temp = (HashSet<String>) string_array.clone();
            temp.remove(temp.toArray()[a]);
            retVal.add(temp);
            retVal.addAll(allCombos(temp));
        }
        retVal.remove(new HashSet<String>());
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
        public String quantifier = "";
        public String fields = "";
        public String from = "";
        public String joins = "";
        private String where = "";
        private String groupby = "";
        public String having = "";
        public String orderby = "";
        public String limit = "";

        public String getSQL() {
            if (tableList.size() > 0) {
                from = " From " + turnArrayToCommaDelimtedList(tableList);
            }
            return "select " + quantifier + fields + from + joins + getWhere()
                    + getGroupby() + having + orderby + limit;
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

        public void setGroupby(String groupby) {
            if (groupby.length() > 0) {
                this.groupby = " group by " + groupby;
            } else {
                this.groupby = "";
            }
        }

        public void addGroupby(String groupby_fragment) {
            this.groupby += groupby_fragment;
        }

        ArrayList<String> tableList = new ArrayList<String>();

    }

}
