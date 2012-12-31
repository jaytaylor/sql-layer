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

package com.akiban.sql.optimizer.rule.cost;

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
                fieldWidth = (int)column.getAverageStorageSize();
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
                fieldWidth = (int) (column.getAverageStorageSize() * PLAUSIBLE_AVERAGE_VAR_USAGE);
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
                fieldWidth = (int)column.getAverageStorageSize();
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
                fieldWidth = (int) (column.getAverageStorageSize() * PLAUSIBLE_AVERAGE_VAR_USAGE);
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
            for (IndexColumn indexColumn : index.getAllColumns()) {
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
