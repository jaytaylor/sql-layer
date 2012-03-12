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

package com.akiban.sql.optimizer.rule.costmodel;

import com.akiban.ais.model.*;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;

public abstract class TreeStatistics
{
    public abstract long rowCount();

    public abstract int rowWidth();

    public abstract RowType rowType();

    public static TreeStatistics forTable(UserTableRowType rowType, TableRowCounts tableRowCounts)
    {
        return new TableStatistics(rowType, tableRowCounts);
    }

    public static TreeStatistics forIndex(IndexRowType rowType, TableRowCounts tableRowCounts)
    {
        return 
            rowType.index().isGroupIndex()
            ? new GroupIndexStatistics(rowType, tableRowCounts)
            : new TableIndexStatistics(rowType, tableRowCounts);
    }

    int fieldWidth(Column column)
    {
        int fieldWidth = 0;
        Type type = column.getType();
        switch (type.akType()) {
            case DATE:
                fieldWidth = 4;
                break;
            case DATETIME:
                fieldWidth = 8;
                break;
            case DECIMAL:
                fieldWidth = column.getMaxStorageSize().intValue();
                break;
            case DOUBLE:
                fieldWidth = 8;
                break;
            case FLOAT:
                fieldWidth = 8;
                break;
            case INT:
                fieldWidth = 8;
                break;
            case LONG:
                fieldWidth = 8;
                break;
            case VARCHAR:
                fieldWidth = (int) (column.getMaxStorageSize() * PLAUSIBLE_AVERAGE_VAR_USAGE);
                break;
            case TEXT:
                fieldWidth = PLAUSIBLE_AVERAGE_BLOB_SIZE;
                break;
            case TIME:
                fieldWidth = 4;
                break;
            case TIMESTAMP:
                fieldWidth = 8;
                break;
            case U_BIGINT:
                fieldWidth = column.getMaxStorageSize().intValue();
                break;
            case U_DOUBLE:
                fieldWidth = 8;
                break;
            case U_FLOAT:
                fieldWidth = 8;
                break;
            case U_INT:
                fieldWidth = 8;
                break;
            case VARBINARY:
                fieldWidth = (int) (column.getMaxStorageSize() * PLAUSIBLE_AVERAGE_VAR_USAGE);
                break;
            case YEAR:
                fieldWidth = 8;
                break;
            case BOOL:
                fieldWidth = 8;
                break;
            case INTERVAL_MILLIS:
                fieldWidth = 4;
                break;
            case INTERVAL_MONTH:
                fieldWidth = 4;
                break;
            default:
                assert false;
        }
        return fieldWidth;
    }

    private static final int PLAUSIBLE_AVERAGE_BLOB_SIZE = 100000;
    private static final double PLAUSIBLE_AVERAGE_VAR_USAGE = 0.3;

    protected TreeStatistics(TableRowCounts tableRowCounts) {
        this.tableRowCounts = tableRowCounts;
    }

    protected final TableRowCounts tableRowCounts;

    private static class TableStatistics extends TreeStatistics
    {
        public long rowCount()
        {
            return tableRowCounts.getTableRowCount(rowType.userTable());
        }

        @Override
        public int rowWidth()
        {
            // Columns
            int rowWidth = 0;
            for (Column column : rowType.userTable().getColumnsIncludingInternal()) {
                rowWidth += fieldWidth(column);
            }
            // Attempt to estimate hkey width
            for (HKeySegment segment : rowType().userTable().hKey().segments()) {
                // ordinal
                rowWidth += 1;
                for (HKeyColumn hKeyColumn : segment.columns()) {
                    rowWidth += fieldWidth(hKeyColumn.column());
                }
            }            
            return rowWidth;
        }

        public RowType rowType()
        {
            return rowType;
        }

        public TableStatistics(UserTableRowType rowType, TableRowCounts tableRowCounts)
        {
            super(tableRowCounts);
            this.rowType = rowType;
        }

        private final UserTableRowType rowType;
    }
    
    private static abstract class IndexStatistics extends TreeStatistics
    {
        @Override
        public final RowType rowType()
        {
            return rowType;
        }

        @Override
        public final int rowWidth()
        {
            int rowWidth = 0;
            Index index = rowType.index();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                rowWidth += fieldWidth(indexColumn.getColumn());
            }
            for (IndexColumn indexColumn : index.getValueColumns()) {
                rowWidth += fieldWidth(indexColumn.getColumn());
            }
            return rowWidth;
        }

        protected IndexStatistics(IndexRowType rowType, TableRowCounts tableRowCounts)
        {
            super(tableRowCounts);
            this.rowType = rowType;
        }
        
        protected final IndexRowType rowType;
    }

    private static class TableIndexStatistics extends IndexStatistics
    {
        public long rowCount()
        {
            TableIndex index = (TableIndex) rowType.index();
            UserTable table = (UserTable) index.getTable();
            return tableRowCounts.getTableRowCount(table);
        }

        public TableIndexStatistics(IndexRowType rowType, TableRowCounts tableRowCounts)
        {
            super(rowType, tableRowCounts);
        }
    }

    private static class GroupIndexStatistics extends IndexStatistics
    {
        public long rowCount()
        {
            GroupIndex index = (GroupIndex) rowType.index();
            return tableRowCounts.getTableRowCount(index.leafMostTable());
        }

        public GroupIndexStatistics(IndexRowType rowType, TableRowCounts tableRowCounts)
        {
            super(rowType, tableRowCounts);
        }
    }
}
