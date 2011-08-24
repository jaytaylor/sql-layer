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

package com.akiban.server.test.it.hapi.randomdb;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.memcache.hprocessor.DefaultProcessedRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

class Expected
{
    public Expected(RCTortureIT test)
    {
        this.test = test;
    }

    public String queryResult(int rootTable,
                              int predicateTable,
                              Column predicateColumn,
                              HapiPredicate.Operator comparison,
                              long literal,
                              boolean indexOrdered) throws HapiRequestException, IOException
    {
        List<NewRow> queryResult = new ArrayList<NewRow>();
        // Find rows belonging to predicate table
        for (NewRow row : test.db) {
            if (row.getTableId() == predicateTable &&
                evaluate(row, predicateTable, predicateColumn, comparison, literal)) {
                queryResult.add(row);
            }
        }
        // Fill in ancestors and descendents
        List<NewRow> relatives = new ArrayList<NewRow>();
        for (NewRow dbRow : test.db) {
            if (!ancestorTypeOf(dbRow.getTableId(), rootTable)) {
                boolean rowAdded = false;
                for (Iterator<NewRow> q = queryResult.iterator(); !rowAdded && q.hasNext();) {
                    NewRow resultRow = q.next();
                    if (ancestorOf(resultRow, dbRow) || ancestorOf(dbRow, resultRow)) {
                        relatives.add(dbRow);
                        rowAdded = true;
                    }
                }
            }
        }
        queryResult.addAll(relatives);
        // Sort
        test.sort(queryResult);
        // If predicate table != root table, then ancestors of rows in predicate table have to be repeated for each
        // row of predicate table
        if (predicateTable != rootTable) {
            List<NewRow> extendedQueryResult = new ArrayList<NewRow>();
            NewRow[] ancestors = new NewRow[2];
            for (NewRow row : queryResult) {
                int rowDepth = depth(row.getTableId());
                if (ancestorTypeOf(row.getTableId(), predicateTable)) {
                    ancestors[rowDepth] = row;
                } else if (ancestorTypeOf(predicateTable, row.getTableId())) {
                    extendedQueryResult.add(row);
                } else {
                    assert row.getTableId() == predicateTable : row;
                    int depth = depth(row.getTableId());
                    for (int i = depth(rootTable); i < depth; i++) {
                        if (ancestors[i] != null && ancestorOf(ancestors[i], row)) {
                            extendedQueryResult.add(ancestors[i]);
                        }
                    }
                    extendedQueryResult.add(row);
                }
            }
            queryResult = extendedQueryResult;
        }
        // Convert to RowData, filling in hkey segment at which each row differs from its predecessor
        List<RowData> rowDatas = new ArrayList<RowData>();
        NewRow previousRow = null;
        for (NewRow row : queryResult) {
            int differSegment = 0;
            if (previousRow != null) {
                int rowTableDepth = depth(row.getTableId());
                if (rowTableDepth < depth(predicateTable)) {
                    differSegment = rowTableDepth == 0 ? 0 : hKeyDepth(row.getTableId());
                } else {
                    differSegment = test.hKey(previousRow).differSegment(test.hKey(row));
                }
            }
            RowData rowData = row.toRowData();
            rowData.differsFromPredecessorAtKeySegment(differSegment);
            rowDatas.add(rowData);
            previousRow = row;
        }
        // Generate json string
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10000);
        test.outputter.output(new DefaultProcessedRequest(test.request,
                                                          test.ddlFunctions().getAIS(test.testSession())),
                              !indexOrdered,
                              rowDatas,
                              outputStream);
        return new String(outputStream.toByteArray());
    }

    private boolean evaluate(NewRow row,
                             int predicateTable,
                             Column predicateColumn,
                             HapiPredicate.Operator comparison,
                             long literal)
    {
        return evaluate(columnValue(row, predicateColumn), comparison, literal);
    }

    private long columnValue(NewRow row, Column column)
    {
        int columnPosition = -1;
        // Regardless of type:
        // - cid is always in position 0
        // - oid, aid are always in position 1
        // - iid is always in position 2
        // - cid_copy is always in the last position
        String columnName = column.columnName();
        if (columnName.equals("cid")) {
            columnPosition = 0;
        } else if (columnName.equals("oid") || columnName.equals("aid")) {
            columnPosition = 1;
        } else if (columnName.equals("iid")) {
            columnPosition = 2;
        } else if (columnName.equals("cid_copy")) {
            columnPosition = row.getRowDef().getFieldCount() - 1;
        } else {
            fail();
        }
        return (Long) row.get(columnPosition);
    }

    private boolean evaluate(long columnValue, HapiPredicate.Operator comparison, long literal)
    {
        switch (comparison) {
            case LT:
                return columnValue < literal;
            case LTE:
                return columnValue <= literal;
            case GT:
                return columnValue > literal;
            case GTE:
                return columnValue >= literal;
            case EQ:
                return columnValue == literal;
        }
        fail();
        return false;
    }

    private boolean ancestorOf(NewRow x, NewRow y)
    {
        boolean ancestor = false;
        int xType = x.getTableId();
        int yType = y.getTableId();
        if (xType == test.customerTable) {
            if (yType != test.customerTable) {
                // Compare cids. This works for order, item, address
                ancestor = y.get(0).equals(x.get(0));
            }
        } else if (xType == test.orderTable) {
            if (yType == test.itemTable) {
                // Compare cids and oids
                ancestor = y.get(0).equals(x.get(0)) && y.get(1).equals(x.get(1));
            }
        }
        return ancestor;
    }

    private boolean ancestorTypeOf(int xType, int yType)
    {
        boolean ancestorType = false;
        if (xType == test.customerTable) {
            ancestorType = yType != test.customerTable;
        } else if (xType == test.orderTable) {
            ancestorType = yType == test.itemTable;
        }
        return ancestorType;
    }

    private int depth(int type)
    {
        return
            type == test.customerTable ? 0 :
            type == test.orderTable ? 1 :
            type == test.itemTable ? 2 :
            type == test.addressTable ? 1 : -1;
    }

    private int hKeyDepth(int type)
    {
        return
            type == test.customerTable ? 0 :
            type == test.orderTable ? 2 :
            type == test.itemTable ? 4 :
            type == test.addressTable ? 2 : -1;
    }

    private RCTortureIT test;
}
