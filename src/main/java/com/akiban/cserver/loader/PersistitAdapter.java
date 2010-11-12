package com.akiban.cserver.loader;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.*;
import com.akiban.cserver.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class PersistitAdapter
{
    public PersistitAdapter(PersistitStore store, GenerateFinalTask task)
            throws PersistitException
    {
        this.store = store;
        this.task = task;
        UserTable leafTable = task.table();
        int nKeySegments = leafTable.getDepth() + 1;
        // Traverse group from leaf table to root. Gather FieldDefs, ordinals and other data needed to construct
        // hkeys later. Accumulate FieldDefs in a stack since we're discovering them backwards, and we don't know
        // how many there will be.
        Stack<FieldDef> hKeyFieldDefStack = new Stack<FieldDef>();
        ordinals = new int[nKeySegments];
        nKeyColumns = new int[nKeySegments];
        RowDefCache rowDefCache = store.getRowDefCache();
        leafRowDef = rowDefCache.getRowDef(leafTable.getName().getDescription());
        RowDef rowDef = leafRowDef;
        UserTable table = leafTable;
        int depth = nKeySegments;
        int hKeyColumns = 0;
        while (table != null) {
            depth--;
            // Find key columns not involved in join with parent. Or, if there is no parent, take all columns.
            // This is the way that RowDefCache.createUserTableRowDef uses PK columns along a path, and therefore
            // with the FieldDefs provided by rowDef.getPkFields().
            Join join = table.getParentJoin();
            List<Column> uniqueKeyColumns = new ArrayList<Column>(table.getPrimaryKey().getColumns());
            if (join != null) {
                List<Column> childJoinColumns = columnsInChild(join.getParent().getPrimaryKey().getColumns(), join);
                uniqueKeyColumns.removeAll(childJoinColumns);
            }
            // Save FieldDefs. Push them in reverse order so they pop in the right order.
            for (int i = uniqueKeyColumns.size() - 1; i >= 0; i--) {
                hKeyFieldDefStack.push(rowDef.getFieldDef(rowDef.getPkFields()[i]));
            }
            // Count hkey columns
            hKeyColumns += uniqueKeyColumns.size();
            // ordinals
            ordinals[depth] = rowDef.getOrdinal();
            nKeyColumns[depth] = uniqueKeyColumns.size();
            table = table.getParentJoin() == null
                    ? null
                    : table.getParentJoin().getParent();
            if (table != null) {
                rowDef = rowDefCache.getRowDef(rowDef.getParentRowDefId());
            }
        }
        assert hKeyFieldDefStack.size() == hKeyColumns : table;
        hKey = new Object[hKeyColumns];
        hKeyFieldDefs = new FieldDef[hKeyColumns];
        int i = 0;
        while (!hKeyFieldDefStack.empty()) {
            hKeyFieldDefs[i++] = hKeyFieldDefStack.pop();
        }
        hKeyColumnPositions = task.hKeyColumnPositions();
        columnPositions = task.columnPositions();
        dbRow = new Object[leafTable.getColumns().size()];
        rowData = new RowData(new byte[ROW_DATA_BUFFER_SIZE]);
        exchange = store.getExchange(leafRowDef, null);
        logState();
    }

    public void handleRow(ResultSet resultSet) throws Exception
    {
        // Populate rowData
        for (int i = 0; i < dbRow.length; i++) {
            dbRow[i] = resultSet.getObject(columnPositions[i] + 1);
            if (resultSet.wasNull()) {
                dbRow[i] = null;
            }
        }
        rowData.createRow(leafRowDef, dbRow);
        rowCount++;
        // Populate hkey
        int i = 0;
        for (int hKeyColumnPosition : hKeyColumnPositions) {
            hKey[i] = resultSet.getObject(hKeyColumnPosition + 1);
            if (resultSet.wasNull()) {
                hKey[i] = null;
            }
            i++;
        }
        // Insert row
        store.writeRowForBulkLoad(exchange, leafRowDef, rowData, ordinals, nKeyColumns, hKeyFieldDefs, hKey);
    }

    public void close() throws InvalidOperationException, PersistitException
    {
        store.updateTableStats(leafRowDef, rowCount);
        store.releaseExchange(exchange);
    }

    // parentColumns are columns that may be present in
    // join.parent. Return the corresponding columns in join.child. If
    // a column is not present in join.parent, it is not represented
    // in the output.
    public static List<Column> columnsInChild(List<Column> parentColumns, Join join)
    {
        List<Column> childColumns = new ArrayList<Column>();
        for (Column parentColumn : parentColumns) {
            Column childColumn = join.getMatchingChild(parentColumn);
            if (childColumn != null) {
                childColumns.add(childColumn);
            }
        }
        return childColumns;
    }

    private void logState()
    {
        StringBuilder ordinalBuffers = new StringBuilder();
        StringBuilder nKeyColumnsBuffer = new StringBuilder();
        StringBuilder fieldDefsBuffer = new StringBuilder();
        ordinalBuffers.append('[');
        nKeyColumnsBuffer.append('[');
        fieldDefsBuffer.append('[');
        int n = ordinals.length;
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                ordinalBuffers.append(", ");
                nKeyColumnsBuffer.append(", ");
            }
            ordinalBuffers.append(ordinals[i]);
            nKeyColumnsBuffer.append(nKeyColumns[i]);
        }
        n = hKeyFieldDefs.length;
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                fieldDefsBuffer.append(", ");
            }
            fieldDefsBuffer.append(hKeyFieldDefs[i].toString());
        }
        ordinalBuffers.append(']');
        nKeyColumnsBuffer.append(']');
        fieldDefsBuffer.append(']');
        logger.info(String.format("ordinals: %s", ordinalBuffers.toString()));
        logger.info(String.format("nKeyColumns: %s", nKeyColumnsBuffer.toString()));
        logger.info(String.format("fieldDefsBuffer: %s", fieldDefsBuffer.toString()));
    }

    private static final Log logger = LogFactory.getLog(PersistitAdapter.class);
    private static final int ROW_DATA_BUFFER_SIZE = 1 << 16; // 64k

    private final PersistitStore store;
    private final GenerateFinalTask task;
    private final RowDef leafRowDef;
    private final FieldDef[] hKeyFieldDefs;
    // The hkey consists of ordinals and key column values. ordinal[i] is followed by nKeyColumns[i] key column values.
    private final int[] ordinals;
    private final int[] nKeyColumns;
    private final int[] hKeyColumnPositions;
    private final int[] columnPositions;
    private final Object[] dbRow;
    private final Object[] hKey;
    private final RowData rowData;
    private final Exchange exchange;
    private long rowCount = 0;
}
