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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;

/*
 * Class setup to allow a systematic approach to creating valid SQL statements 
 * */
public class SQLStatementGenerator {

    private ArrayList<String> root_string_array;
    private ArrayList<String> root_int_array;
    private ArrayList<String> root_dt_array;

    public ArrayList<String> getRoot_array() {
        ArrayList<String> retVal = new ArrayList<String>();
        if (root_string_array != null) {
            retVal.addAll(root_string_array);
        }
        if (root_int_array != null) {
            retVal.addAll(root_int_array);
        }
        if (root_dt_array != null) {
            retVal.addAll(root_dt_array);
        }
        return retVal;

    }

    public void setRoot_int_array(String[] root_int_array) {
        setRoot_int_array(new ArrayList<String>(Arrays.asList(root_int_array)));
    }

    public void setRoot_int_array(ArrayList<String> root_int_array) {
        this.root_int_array = root_int_array;
        this.setFields(getRoot_array());
        this.setTableList(getRoot_array().toString());
    }

    public void setRoot_dt_array(String[] root_dt_array) {
        setRoot_dt_array(new ArrayList<String>(Arrays.asList(root_dt_array)));
    }

    public void setRoot_dt_array(ArrayList<String> arrayList) {
        this.root_dt_array = arrayList;
        this.setFields(getRoot_array());
        this.setTableList(getRoot_array().toString());
    }

    public void setRoot_string_array(String[] root_string_array) {
        setRoot_string_array(new ArrayList<String>(
                Arrays.asList(root_string_array)));
    }

    public void setRoot_string_array(ArrayList<String> str_list) {
        this.root_string_array = str_list;
        this.setFields(getRoot_array());
        this.setTableList(getRoot_array().toString());
    }

    public ArrayList<String> getRoot_int_array() {
        return root_int_array;
    }

    public ArrayList<String> getRoot_dt_array() {
        return root_dt_array;
    }

    public ArrayList<String> getRoot_string_array() {
        return root_string_array;
    }

    private String quantifier = "";
    private String fields = "";
    private String from = "";
    private Hashtable<Integer, String> joins = new Hashtable<Integer, String>();
    private String where = "";
    private String groupby = "";
    private String having = "";
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

        return "select " + quantifier + getFields() + from + getJoins() + getWhere()
                + getGroupby() + having + getOrderby() + getLimit();
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String p_where) {
        if (p_where.trim().length() > 0) {
            this.where = " where " + p_where + " ";
        }
    }

    public void addWhere(String where_fragment) {
        if (!where.startsWith(" where")) {
            this.where = " where " + where_fragment + " ";
        } else {
            this.where += " AND " + where_fragment + " ";
        }
    }

    public void orWhere(String where_fragment) {
        if (!where.startsWith(" where")) {
            this.where = " where " + where_fragment + " ";
            ;
        } else {
            this.where += " OR " + where_fragment + " ";
        }
    }

    public String getGroupby() {
        return groupby;
    }

    public void setGroupby(ArrayList<String> group_by_fields) {
        if (group_by_fields.size() > 0) {
            this.groupby = " group by "
                    + turnArrayToCommaDelimtedList(group_by_fields);
        } else {
            this.groupby = "";
        }
    }

    public void addGroupby(String groupby_fragment) {
        this.groupby += ", " + groupby_fragment;
    }

    public String getOrderby() {
        return orderby;
    }

    public void setOrderby(ArrayList<String> p_order_by) {
        if (p_order_by.size() > 0) {
            this.orderby = " order by "
                    + turnArrayToCommaDelimtedList(p_order_by);
        } else {
            this.orderby = "";
        }
    }

    public String getFields() {
        return fields;
    }

    public void setFields(ArrayList<String> p_select_fields) {
        this.fields = turnArrayToCommaDelimtedList(p_select_fields);

    }

    ArrayList<String> getTableList() {
        return tableList;
    }

    void setTableList(String p_fields) {
        this.tableList = extractTableList(p_fields);
    }

    void setTableListConvert(ArrayList<String> p_fields_list) {
        this.tableList = extractTableList(p_fields_list.toString());
    }

    public String getLimit() {

        return limit;
    }

    public void clearLimit() {
        this.limit = "";
    }

    public void setLimit(int p_limit) {
        if (p_limit > 0) {
            this.limit = " LIMIT " + p_limit;
        }
    }

    public String getQuantifier() {
        return quantifier;
    }

    public void setQuantifier(String quantifier) {
        this.quantifier = quantifier;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getJoins() {
        String retVal = "";
        for (int x=0;x < joins.size();x++) {
            if (joins.get(x) != null ) {
                retVal += joins.get(x);
            }
        }
        return retVal;
    }

    public void clearJoins(int position) {
        this.joins.remove(position);
    }
    
    public void addJoins(int position, String joins) {
        this.joins.put(position, joins);
    }
    
    public void setJoins(int position, String joins) {
        this.joins.put(position, joins);
    }

    public String getHaving() {
        return having;
    }

    public void setHaving(String having) {
        this.having = having;
    }

    public void setGroupby(String groupby) {
        this.groupby = groupby;
    }

    public void setTableList(ArrayList<String> tableList) {
        this.tableList = tableList;
    }

    protected String turnArrayToCommaDelimtedList(HashSet<String> string_array) {
        return turnArrayToCommaDelimtedList(new ArrayList<String>(string_array));
    }

    protected String turnArrayToCommaDelimtedList(ArrayList<String> arrayList) {
        String retVal = "";
        if (arrayList.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (String s : arrayList) {
                sb.append(s);
                sb.append(",");
            }
            retVal = sb.toString();
            retVal = retVal.substring(0, retVal.length() - 1);
        }
        return retVal;
    }

    protected ArrayList<String> extractTableList(String sql) {
        ArrayList<String> retVal = new ArrayList<String>();
        sql = sql.replace("[", "");
        String[] data = sql.split("[,]");
        if (data.length > 0 && sql.trim().length() > 0) {
            for (int x = 0; x < data.length; x++) {
                if (data[x].lastIndexOf(".") > 0) {
                    String table = data[x]
                            .substring(0, data[x].lastIndexOf("."))
                            .toUpperCase().trim();
                    if (!retVal.contains(table)) {
                        retVal.add(table);
                    }
                }
            }
        }
        return retVal;
    }

    public void setFields(HashSet<String> string_array) {
        this.setFields(new ArrayList<String>(string_array));

    }

    public void setRoot_string_array(HashSet<String> row) {
        setRoot_string_array(new ArrayList<String>(row));
        setTableListConvert(getRoot_array());        
    }

    public void fixFrom(String primaryTable) {
        int spot = this.tableList.indexOf(primaryTable.toUpperCase());
        if (spot != this.tableList.size()) {
            Collections.swap(this.tableList, spot, this.tableList.size() - 1);
        }

    }

}
