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
import java.util.Date;

/*
 * creates tests for bitwise math operations
 * */
public class AllQueryComboCreator extends GenericCreator implements Runnable {

    private static final String TARGET_AREA = "all-bitwise-matrix";
    StringBuilder yaml_file_contents = new StringBuilder();
    String server = "localhost";
    String username = "root";
    String password = "";

    /**
     * @param args
     */
    public static void main(String[] args) {
        AllQueryComboCreator b = new AllQueryComboCreator();
        b.run();
    }

    final String[] QUANTIFIERS = { "Distinct", "All", "" };
    final String[] INT_FIELDS = { "", "customers.customer_id" };
    final String[] STR_FIELDS = { "", "customers.customer_title",
            "customers.customer_name", "customers.primary_payment_code",
            "customers.payment_status", "orders.ship_priority" };
    final String[] DT_FIELDS = { "", "orders.order_date", "orders.ship_date" };
    final String[] TABLES = { "customers", "orders" };
    final String[] INT_PARAMS = { "-5", "0", "2", "99" };
    final String[] STR_PARAMS = { "ABCD", "F$%", "CCCCC" };
    final String[] DT_PARAMS = { "1999-03-04", "2012-12-31" };
    final String[] FUNCTION_LIST = { "[i] MOD [i]", "AVG([i],[i],[i])",
            "CONCAT([s],[s])", "DATE_ADD([d], INTERVAL [i] HOUR)" };
    final Relationship[] RELATIONSHIPS = { new Relationship("customers",
            "orders", "cid", "cid") };

    //  SELECT [ <set quantifier> ] <select list> <table expression>
    //    <table expression>    ::= 
    //            <from clause>
    //            [ <where clause> ]
    //            [ <group by clause> ]
    //            [ <having clause> ]

    @Override
    public void run() {
        header();

        String quan = "";
        String from = "";
        ArrayList<ArrayList<String>> select_fields = prepareFieldList();

        for (int x = 0; x < select_fields.size(); x++) {
            from = extractFromClause(select_fields.get(x).toString());

            for (int a = 0; a < QUANTIFIERS.length; a++) {
                quan = " " + QUANTIFIERS[a] + " ";
                for (int b = 0; b < select_fields.get(x).size(); b++) {
                    ArrayList<String> temp_select_fields = select_fields.get(x);
                    temp_select_fields.set(b, quan + temp_select_fields.get(b));

                    String full_sql = "select "
                            + turnArrayToCommaDelimtedList(select_fields.get(x))
                            + from;
                    writeYamlBlock(full_sql);
                    temp_select_fields = select_fields.get(x);
                    temp_select_fields.set(b,
                            " count(" + temp_select_fields.get(b) + ") ");
                    full_sql = "select "
                            + turnArrayToCommaDelimtedList(select_fields.get(x))
                            + from;
                    writeYamlBlock(full_sql);
                }
            }
        }

        finalizeFile();
    }

    private ArrayList<ArrayList<String>> prepareFieldList() {

        ArrayList<ArrayList<String>> retVal = new ArrayList<ArrayList<String>>();
        ArrayList<String> row = new ArrayList<String>();
        for (int b = 0; b < INT_FIELDS.length; b++) {
            if (!INT_FIELDS[b].equals("")) {
                row.add(INT_FIELDS[b]);
            }

            for (int c = 0; c < STR_FIELDS.length; c++) {
                if (!STR_FIELDS[c].equals("")) {
                    row.add(STR_FIELDS[c]);
                }
                for (int d = 0; d < DT_FIELDS.length; d++) {
                    if (!DT_FIELDS[d].equals("")) {
                        row.add(DT_FIELDS[d]);
                    }
                    retVal.add(row);
                }
            }
        }
        return retVal;
    }

    protected String extractFromClause(String sql) {
        String retVal = " from ";
        sql = sql.trim();
        if (sql.contains(",")) {
            String[] temp = sql.split(",");
            for (int x = 0; x < temp.length; x++) {
                String[] temp2 = temp[x].split("[.]");
                for (int y = 0; y < temp2.length; y += 2) {
                    String temp_str = temp2[y].trim();
                    if (temp2[y].indexOf(" ") > 0) {
                        temp_str = temp2[y].substring(temp2[y].indexOf(" "));
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
    }

    private String callMySQL(String sql) {
        String retVal = "- output: ";
        try {
            retVal = generateOutputFromInno(server, username, password, sql,
                    null);
        } catch (Exception e) {
            System.out.println(e.getMessage());
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
        String path = System.getProperty("user.dir")
                + "/src/test/resources/com/akiban/sql/pg/yaml/functional/";
        //recordYamlToDisk(path);
        System.out.println(yaml_file_contents.toString());
    }

    private void header() {
        yaml_file_contents
                .append("# generated by com.akiban.sql.test.AllQueryComboCreator on "
                        + new Date() + eol);
        yaml_file_contents.append("---" + eol + "- Include: all-" + TARGET_AREA
                + "-schema.yaml" + eol);
        yaml_file_contents.append("..." + eol);
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
