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

    private static final String TARGET_AREA = "query-combo";
    StringBuilder yaml_file_contents = new StringBuilder();
    String server = "localhost";
    String username = "root";
    String password = "";
    int counter = 0;

    /**
     * @param args
     */
    public static void main(String[] args) {
        AllQueryComboCreator b = new AllQueryComboCreator();
        b.run();
    }

    final String[] QUANTIFIERS = { "Distinct", "All", "" };
    final String[] INT_FIELDS = { "customers.customer_id" };
    final String[] STR_FIELDS = { "customers.customer_title",
            "customers.customer_name", "customers.primary_payment_code",
            "customers.payment_status", "orders.ship_priority" };
    final String[] DT_FIELDS = { "orders.order_date", "orders.ship_date" };
    final String[] TABLES = { "customers", "orders" };
    final String[] INT_PARAMS = { "-5", "0", "2", "99" };
    final String[] STR_PARAMS = { "ABCD", "FONDUE", "CCCCC", "ABDFTERE" };
    final String[] DT_PARAMS = { "1999-03-04", "2012-12-31" };
    final String[] FUNCTION_LIST = { "[i]=[i] MOD [i]", "[i]=AVG([i],[i],[i])",
            "[s]=%1$s = CONCAT('%2$s','%3$s')",
            "[d]=DATE_ADD([d], INTERVAL [i] HOUR)" };
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

        justWhereIterationsString(new HashSet<String>(Arrays.asList(STR_FIELDS)));
        justWhereIterationsInteger(new HashSet<String>(
                Arrays.asList(INT_FIELDS)));
        justWhereIterationsDate(new HashSet<String>(Arrays.asList(DT_FIELDS)));

        finalizeFile();
    }

    private void justWhereIterationsInteger(HashSet<String> hashSet) {
        // TODO Auto-generated method stub

    }

    private void justWhereIterationsDate(HashSet<String> hashSet) {
        // TODO Auto-generated method stub

    }

    private void justWhereIterationsString(HashSet<String> string_array) {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb, Locale.US);

        String from = extractFromClause(string_array.toString());
        Iterator<String> i = string_array.iterator();
        while (i.hasNext()) {
            String field = i.next();

            String full_sql = "select " + field + from;
            for (int a = 0; a < FUNCTION_LIST.length; a++) {
                String function = new String(FUNCTION_LIST[a]).substring(4);
                if (FUNCTION_LIST[a].startsWith("[s]=")) {
                    for (int x = 0; x < STR_PARAMS.length; x++) {
                        formatter.format(function, field, STR_PARAMS[x],
                                STR_PARAMS[Math.min(STR_PARAMS.length - 1,
                                        (x + 1))]);
                        writeYamlBlock(full_sql + " where " + sb.toString());
                        sb.setLength(0);
                    }
                }
            }
        }

    }

    private void justSelectIterations(HashSet<HashSet<String>> select_fields) {
        String quan;
        String from;
        Iterator<HashSet<String>> i = select_fields.iterator();
        while (i.hasNext()) {
            ArrayList<String> next = new ArrayList<String>(i.next());

            from = extractFromClause(next.toString());

            String full_sql = "select " + turnArrayToCommaDelimtedList(next)
                    + from;
            writeYamlBlock(full_sql);

            for (int a = 0; a < QUANTIFIERS.length; a++) {
                quan = " " + QUANTIFIERS[a] + " ";

                for (int b = 0; b < next.size(); b++) {
                    @SuppressWarnings("unchecked")
                    ArrayList<String> temp_select_fields = (ArrayList<String>) next
                            .clone();

                    if (QUANTIFIERS[a].length() > 0) {
                        Collections.swap(next, 0, b);
                        temp_select_fields = (ArrayList<String>) next.clone();
                        temp_select_fields.set(0,
                                quan + temp_select_fields.get(b));

                        full_sql = "select "
                                + turnArrayToCommaDelimtedList(temp_select_fields)
                                + from;
                        writeYamlBlock(full_sql);

                        temp_select_fields = (ArrayList<String>) next.clone();
                        temp_select_fields.set(0, " count("
                                + temp_select_fields.get(b) + ") ");

                        full_sql = "select "
                                + turnArrayToCommaDelimtedList(temp_select_fields)
                                + from;
                        writeYamlBlock(full_sql);

                        temp_select_fields = (ArrayList<String>) next.clone();
                        temp_select_fields.set(0, " count(distinct "
                                + temp_select_fields.get(b) + ") ");

                        full_sql = "select "
                                + turnArrayToCommaDelimtedList(temp_select_fields)
                                + from;
                        writeYamlBlock(full_sql);
                    }
                }
            }
        }
    }

    protected String extractFromClause(String sql) {
        String retVal = " from ";
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
                            retVal += temp_str + " , ";
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
                        retVal += temp_str.trim() + " , ";
                    }
                }
            }
        }
        if (!retVal.equals(" from ")) {
            retVal = " "
                    + retVal.trim().substring(0, retVal.trim().length() - 1);
        } else {
            retVal = " from dual ";
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
        } catch (Exception e) {
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
        String path = System.getProperty("user.dir")+"/";
        recordYamlToDisk(path);
        System.out.println(yaml_file_contents.toString());
        System.out.println("Test generated is " + counter);
    }

    private void header() {
        yaml_file_contents
                .append("# generated by com.akiban.sql.test.AllQueryComboCreator on "
                        + new Date() + eol);
        yaml_file_contents.append("---" + eol + "- Include: all-caoi-schema.yaml" + eol);
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

}
