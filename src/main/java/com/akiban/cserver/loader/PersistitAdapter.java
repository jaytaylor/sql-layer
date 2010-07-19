package com.akiban.cserver.loader;

import java.sql.ResultSet;

import com.akiban.ais.model.UserTable;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.StoreException;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public class PersistitAdapter
{
    public PersistitAdapter(PersistitStore store, GenerateFinalTask task)
            throws PersistitException
    {
        this.store = store;
        UserTable leafTable = task.table();
        int nKeySegments = leafTable.getDepth() + 1;
        // Traverse group from table to root. Gather RowDefs, ordinals, and
        // create the hKey structure.
        fieldDefs = new FieldDef[nKeySegments][];
        ordinals = new int[nKeySegments];
        hKey = new Object[nKeySegments][];
        RowDefCache rowDefCache = store.getRowDefCache();
        leafRowDef = rowDefCache
                .getRowDef(leafTable.getName().getDescription());
        RowDef rowDef = leafRowDef;
        UserTable table = leafTable;
        int depth = nKeySegments;
        while (table != null) {
            depth--;
            int nPKColumns = table.getPrimaryKey().getColumns().size();
            // fieldDefs
            fieldDefs[depth] = new FieldDef[nPKColumns];
            for (int i = 0; i < nPKColumns; i++) {
                fieldDefs[depth][i] = rowDef
                        .getFieldDef(rowDef.getPkFields()[i]);
            }
            // hkey
            hKey[depth] = new Object[nPKColumns];
            // ordinals
            ordinals[depth] = rowDef.getOrdinal();
            table = table.getParentJoin() == null ? null : table
                    .getParentJoin().getParent();
            if (table != null) {
                rowDef = rowDefCache.getRowDef(rowDef.getParentRowDefId());
            }
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
        int t = 0; // table
        int f = 0; // key field of table
        Object[] h = hKey[0]; // hkey fields for table at root
        for (int hKeyColumnPosition : hKeyColumnPositions) {
            h[f] = resultSet.getObject(hKeyColumnPosition + 1);
            if (resultSet.wasNull()) {
                h[f] = null;
            }
            if (++f == h.length) {
                if (++t == hKey.length) {
                    t = -1;
                    f = -1;
                } else {
                    h = hKey[t];
                    f = 0;
                }
            }
        }
        // Insert row
        store.writeRowForBulkLoad(exchange, leafRowDef, rowData, ordinals,
                fieldDefs, hKey);
    }

    public void close() throws StoreException, PersistitException
    {
        store.updateTableStats(leafRowDef, rowCount);
        store.releaseExchange(exchange);
    }

    private static final int ROW_DATA_BUFFER_SIZE = 1 << 16; // 64k

    private final PersistitStore store;
    private final RowDef leafRowDef;
    private final FieldDef[][] fieldDefs;
    private final int[] ordinals;
    private final int[] hKeyColumnPositions;
    private final int[] columnPositions;
    private final Object[] dbRow;
    private final Object[][] hKey;
    private final RowData rowData;
    private final Exchange exchange;
    private long rowCount = 0;
}
