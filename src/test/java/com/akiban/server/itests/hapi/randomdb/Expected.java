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

package com.akiban.server.itests.hapi.randomdb;

import com.akiban.server.RowData;
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
                              Comparison comparison,
                              long literal) throws HapiRequestException, IOException
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
        for (NewRow row : test.db) {
            if (!ancestorTypeOf(row.getTableId(), rootTable)) {
                boolean rowAdded = false;
                for (Iterator<NewRow> q = queryResult.iterator(); !rowAdded && q.hasNext();) {
                    NewRow resultRow = q.next();
                    if (ancestorOf(resultRow, row) || ancestorOf(row, resultRow)) {
                        relatives.add(row);
                        rowAdded = true;
                    }
                }
            }
        }
        queryResult.addAll(relatives);
        // Sort
        test.sort(queryResult);
        // Convert to RowData, filling in hkey segment at which each row differs from its predecessor
        List<RowData> rowDatas = new ArrayList<RowData>();
        NewRow previousRow = null;
        for (NewRow row : queryResult) {
            int differSegment = 0;
            if (previousRow != null) {
                differSegment = test.hKey(previousRow).differSegment(test.hKey(row));
                assertTrue(differSegment > 0);
            }
            RowData rowData = row.toRowData();
            rowData.differsFromPredecessorAtKeySegment(differSegment);
            rowDatas.add(rowData);
            previousRow = row;
        }
        // Generate json string
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10000);
        test.outputter.output(new DefaultProcessedRequest(test.request, test.testSession(), test.ddlFunctions()),
                         rowDatas,
                         outputStream);
        return new String(outputStream.toByteArray());
    }

    private boolean evaluate(NewRow row,
                             int predicateTable,
                             Column predicateColumn,
                             Comparison comparison,
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

    private boolean evaluate(long columnValue, Comparison comparison, long literal)
    {
        switch (comparison) {
            case LT:
                return columnValue < literal;
            case LE:
                return columnValue <= literal;
            case GT:
                return columnValue > literal;
            case GE:
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

    private RCTortureIT test;
}
