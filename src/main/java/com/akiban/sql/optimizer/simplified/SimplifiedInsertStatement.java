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

package com.akiban.sql.optimizer.simplified;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.akiban.server.error.NoSuchColumnException;
import com.akiban.sql.parser.InsertNode;
import com.akiban.sql.parser.ResultColumn;
import com.akiban.sql.parser.ResultColumnList;
import com.akiban.sql.parser.ValueNode;

import com.akiban.ais.model.Column;

/**
 * An SQL INSERT statement turned into a simpler form for the interim
 * heuristic optimizer.
 * 
 * TODO: These items are still left to be implemented
 * * CHECK CONSTRAINTS including NOT NULL check as a expression on top of the column ones
 * * DEFAULT VALUES, including AUTO-INCREMENT
 * 
 */
public class SimplifiedInsertStatement extends SimplifiedTableStatement
{
    protected static final SimpleExpression NULL_EXPR = new LiteralExpression(null);
    private List<TargetColumn> targetColumns = null;
    ColumnExpressionToIndex fieldOffsets;
    
    public SimplifiedInsertStatement(InsertNode insert, Set<ValueNode> joinConditions) {
        super(insert, joinConditions);
        
        if (insert.getOrderByList() != null)
            fillFromOrderBy(insert.getOrderByList());
        if (insert.getOffset() != null)
            fillOffset(insert.getOffset());
        if (insert.getFetchFirst() != null) 
            fillLimit(insert.getOffset());
        
        if (insert.getTargetColumnList() != null)
            fillTargetFromList (insert.getTargetColumnList());
        else 
            fillTargetFromDDL();
    }
    
    /**
     * Fill the targetColumns from the list of columns provided by the user. 
     * e.g. INSERT INTO t1 (c1, c4, c11, c3). 
     * This lets the user insert items in any order, so this list create the
     * mapping between the user specified column order and the DDL expected one
     * @param rcl SQL parser list of user supplied columns. 
     */
    protected void fillTargetFromList (ResultColumnList rcl) {
       
       int table_cols = getTargetTable().getTable().getColumns().size();
       targetColumns = new ArrayList<TargetColumn>(table_cols);
        Map<Column,Integer> columnOffsets = new HashMap<Column,Integer>(table_cols);
        int i = 0;
        for (ResultColumn resultColumn : rcl) {
            Column column = getTargetTable().getTable().getColumn(resultColumn.getName());
            if (column == null) {
                throw new NoSuchColumnException (resultColumn.getName());
            }
            targetColumns.add(new TargetColumn(column, getColumnExpression(column)));
            columnOffsets.put(column, i++);
        }
        
        // If the list of columns provided does not cover all of the columns in the 
        // in the table, add the remaining columns. 
        if (i < table_cols) {
            for (Column column : getTargetTable().getTable().getColumns()) {
                if (!columnOffsets.containsKey(column)) {
                    columnOffsets.put(column, i++);
                    //TODO use Column.defaultValue expression if exists. 
                    targetColumns.add(new TargetColumn(column, NULL_EXPR));
                }
            }
        }
        fieldOffsets = new ColumnIndexMap (columnOffsets);
    }

   /**
    * fillTargetFromDDL is used when the INSERT statement lacks the list of columns
    * e.g. INSERT INTO T1 <(c1, c2, c3)>. If the list of columns is omitted, SQL 
    * assumes the values (or query results) are in the order of columns defined in the 
    * CREATE TABLE (i.e. DDL order). 
    */
   protected void fillTargetFromDDL() {
       int table_cols = getTargetTable().getTable().getColumns().size();
       int max_cols = (getSelectColumns() != null ? getSelectColumns().size() : 
                       getValues() != null ? getValues().get(0).size() : 
                       table_cols);
       if (max_cols > table_cols) {
           // TODO: throw new TooManyColumnsException(); ?
           max_cols = table_cols; 
       }
       targetColumns = new ArrayList<TargetColumn>(table_cols);
       Map<Column,Integer> columnOffsets = new HashMap<Column,Integer>(table_cols);
       int i = 0;
       for (Column column : getTargetTable().getTable().getColumns()) {
           columnOffsets.put(column, i++);
           // if the number of columns in the select list or values list is 
           // smaller than the number of columns in the table, use the smaller
           // list. 
           
           if (i <= max_cols) {
               targetColumns.add(new TargetColumn(column, getColumnExpression(column)));
           } else {
               // TODO: when column has defaultValue expression, use that instead
               targetColumns.add(new TargetColumn(column, NULL_EXPR));
           }
       }
       fieldOffsets = new ColumnIndexMap(columnOffsets);
   }
    public List<TargetColumn> getTargetColumns() {
        return targetColumns;
    }
    
    public ColumnExpressionToIndex getFieldOffset() {
        return fieldOffsets;
    }
    
    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        str.append("\ntarget: ");
        str.append(getTargetTable());
        str.append("\ncolumns: [");
        for (int i = 0; i < getTargetColumns().size(); i++) {
            if (i > 0) str.append(", ");
            str.append(getTargetColumns().get(i));
        }
        str.append("]");
        str.append("\ngroup: ");
        str.append(getGroup());
        if (getJoins() != null) {
            str.append("\njoins: ");
            str.append(getJoins());
        }
        else if (getValues() != null) {
            str.append("\nvalues: ");
            str.append(getValues());
        }
        if (getSelectColumns() != null) {
            str.append("\nselect: [");
            for (int i = 0; i < getSelectColumns().size(); i++) {
                if (i > 0) str.append(", ");
                str.append(getSelectColumns().get(i));
            }
            str.append("]");
        }
        if (!getConditions().isEmpty()) {
            str.append("\nconditions: ");
            for (int i = 0; i < getConditions().size(); i++) {
                if (i > 0) str.append(",\n  ");
                str.append(getConditions().get(i));
            }
        }
        if (getSortColumns() != null) {
            str.append("\nsort: ");
            for (int i = 0; i < getSortColumns().size(); i++) {
                if (i > 0) str.append(", ");
                str.append(getSortColumns().get(i));
            }
        }
        if (getOffset() > 0) {
            str.append("\noffset: ");
            str.append(getOffset());
        }
        if (getLimit() >= 0) {
            str.append("\nlimit: ");
            str.append(getLimit());
        }
        str.append("\nequivalences: ");
        for (int i = 0; i < getColumnEquivalences().size(); i++) {
            if (i > 0) str.append(",\n  ");
            int j = 0;
            for (Column column : getColumnEquivalences().get(i)) {
                if (j++ > 0) str.append(" = ");
                str.append(column);
            }
        }
        return str.toString();
    }
}
