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

package com.akiban.qp.memoryadapter;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.statistics.IndexStatistics;

public abstract class BasicFactoryBase implements MemoryTableFactory {
    private final TableName sourceTable;
    
    public BasicFactoryBase(TableName sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public TableName getName() {
        return sourceTable;
    }

    @Override
    public Cursor getIndexCursor(Index index, Session session,
            IndexKeyRange keyRange, Ordering ordering,
            IndexScanSelector scanSelector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index) {
        throw new UnsupportedOperationException();
    }

    public RowType getRowType(MemoryAdapter adapter) {
        return adapter.schema().userTableRowType(adapter.schema().ais().getUserTable(sourceTable));
    }
    
    public static String boolResult(boolean bool) {
        return bool ? "YES" : "NO";
    }
    public static final int IDENT_MAX = 128;
}
