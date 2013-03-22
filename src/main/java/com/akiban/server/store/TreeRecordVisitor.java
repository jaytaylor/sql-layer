
package com.akiban.server.store;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TreeRecordVisitor
{
    public final void visit() throws PersistitException, InvalidOperationException
    {
        NewRow row = row();
        Object[] key = key(row.getRowDef());
        visit(key, row);
    }

    public final void initialize(Session session, PersistitStore store, Exchange exchange)
    {
        this.session = session;
        this.store = store;
        this.exchange = exchange;
        for (UserTable table : store.getAIS(session).getUserTables().values()) {
            if (!table.getName().getSchemaName().equals(TableName.INFORMATION_SCHEMA) &&
                !table.getName().getSchemaName().equals(TableName.SECURITY_SCHEMA)) {
                ordinalToTable.put(table.rowDef().getOrdinal(), table);
            }
        }
    }

    public abstract void visit(Object[] key, NewRow row);

    private NewRow row() throws PersistitException, InvalidOperationException
    {
        RowData rowData = new RowData(EMPTY_BYTE_ARRAY);
        store.expandRowData(exchange, rowData);
        return new LegacyRowWrapper(store.getRowDef(session, rowData.getRowDefId()), rowData);
    }

    private Object[] key(RowDef rowDef)
    {
        // Key traversal
        Key key = exchange.getKey();
        int keySize = key.getDepth();
        // HKey traversal
        HKey hKey = rowDef.userTable().hKey();
        List<HKeySegment> hKeySegments = hKey.segments();
        int k = 0;
        // Traverse key, guided by hKey, populating result
        Object[] keyArray = new Object[keySize];
        int h = 0;
        while (k < hKeySegments.size()) {
            HKeySegment hKeySegment = hKeySegments.get(k++);
            UserTable table = hKeySegment.table();
            int ordinal = (Integer) key.decode();
            assert ordinalToTable.get(ordinal) == table : ordinalToTable.get(ordinal);
            keyArray[h++] = table;
            for (int i = 0; i < hKeySegment.columns().size(); i++) {
                keyArray[h++] = key.decode();
            }
        }
        return keyArray;
    }

    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private Session session;
    private PersistitStore store;
    private Exchange exchange;
    private final Map<Integer, UserTable> ordinalToTable = new HashMap<>();
}
