package com.akiban.cserver.loader;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.StoreException;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PersistitAdapter
{
    public PersistitAdapter(PersistitStore store, GenerateFinalTask task)
            throws PersistitException
    {
        this.store = store;
        UserTable leafTable = task.table();
        int nKeySegments = leafTable.getDepth() + 1;
        logger.info(String.format("Leaf table %s, key segments: %s", leafTable, nKeySegments));
        // Traverse group from leaf table to root. Gather FieldDefs, ordinals and other data needed to construct
        // hkeys later. Accumulate FieldDefs in a stack since we're discovering them backwards, and we don't know
        // how many there will be.
        Stack<FieldDef> fieldDefStack = new Stack<FieldDef>();
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
                logger.info(String.format("    join: %s", join));
                logger.info(String.format("    join parent PK: %s", join.getParent().getPrimaryKey().getColumns()));
                List<Column> childJoinColumns = Task.columnsInChild(join.getParent().getPrimaryKey().getColumns(), join);
                logger.info(String.format("    child join columns: %s", childJoinColumns));
                uniqueKeyColumns.removeAll(childJoinColumns);
            }
            logger.info(String.format("    table: %s, depth: %s, uniqueKeyColumns: %s",
                                      table, depth, uniqueKeyColumns));
            // Save FieldDefs. Push them in reverse order so they pop in the right order.
            for (int i = uniqueKeyColumns.size() - 1; i >= 0; i--) {
                fieldDefStack.push(rowDef.getFieldDef(rowDef.getPkFields()[i]));
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
        hKey = new Object[hKeyColumns];
        fieldDefs = new FieldDef[fieldDefStack.size()];
        int i = 0;
        while (!fieldDefStack.empty()) {
            fieldDefs[i++] = fieldDefStack.pop();
        }
        hKeyColumnPositions = task.hKeyColumnPositions();
        columnPositions = task.columnPositions();
        dbRow = new Object[leafTable.getColumns().size()];
        rowData = new RowData(new byte[ROW_DATA_BUFFER_SIZE]);
        exchange = store.getExchange(leafRowDef, null);
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
        store.writeRowForBulkLoad(exchange, leafRowDef, rowData, ordinals, nKeyColumns, fieldDefs, hKey);
    }

    public void close() throws StoreException, PersistitException
    {
        store.updateTableStats(leafRowDef, rowCount);
        store.releaseExchange(exchange);
    }

    private static final Log logger = LogFactory.getLog(Tracker.class);

    private static final int ROW_DATA_BUFFER_SIZE = 1 << 16; // 64k

    private final PersistitStore store;
    private final RowDef leafRowDef;
    private final FieldDef[] fieldDefs;
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
