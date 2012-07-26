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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.util.tap.InOutTap;

public abstract class StoreAdapter
{
    public abstract GroupCursor newGroupCursor(GroupTable groupTable);

    public abstract Cursor newIndexCursor(QueryContext context,
                                          Index index,
                                          IndexKeyRange keyRange,
                                          API.Ordering ordering,
                                          IndexScanSelector scanSelector,
                                          boolean usePValues);

    public abstract <HKEY extends com.akiban.qp.row.HKey> HKEY newHKey(HKey hKeyMetadata);

    public final Schema schema()
    {
        return schema;
    }

    public abstract Store getUnderlyingStore();

    public abstract void updateRow(Row oldRow, Row newRow, boolean usePValues);
    
    public abstract void writeRow (Row newRow, boolean usePValues);
    
    public abstract void deleteRow (Row oldRow, boolean usePValues);

    public abstract Cursor sort(QueryContext context,
                                Cursor input,
                                RowType rowType,
                                API.Ordering ordering,
                                API.SortOption sortOption,
                                InOutTap loadTap,
                                boolean usePValues);

    public long getQueryTimeoutSec() {
        return config.queryTimeoutSec();
    }

    public abstract long rowCount(RowType tableType);
    
    public abstract long sequenceNextValue(TableName sequenceName);

    public final Session getSession() {
        return session;
    }

    public enum AdapterType {
        PERSISTIT_ADAPTER,
        MEMORY_ADAPTER;
    }
    
    public final ConfigurationService getConfig() {
        return config;
    }

    protected StoreAdapter(Schema schema,
            Session session,
            ConfigurationService config)
    {
        this.schema = schema;
        this.session = session;
        this.config = config;
    }

    // Object state

    protected final Schema schema;
    private final Session session;
    private final ConfigurationService config;

}
