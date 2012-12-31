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
            if (!table.getName().getSchemaName().equals(TableName.INFORMATION_SCHEMA)) {
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
    private final Map<Integer, UserTable> ordinalToTable = new HashMap<Integer, UserTable>();
}
