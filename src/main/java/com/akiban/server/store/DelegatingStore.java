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

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

import java.util.Collection;

public abstract class DelegatingStore<S extends Store & Service> implements Store, Service {

    private final S delegate;

    public DelegatingStore(S delegate) {
        this.delegate = delegate;
    }

    protected S getDelegate() {
        return delegate;
    }

    // Store interface -- non-delegating

    // Store interface -- auto-generated

    public void start() {
        delegate.start();
    }

    public void stop() {
        delegate.stop();
    }

    public void crash() {
        delegate.crash();
    }

    public RowDefCache getRowDefCache() {
        return delegate.getRowDefCache();
    }

    public void writeRow(Session session, RowData rowData) throws PersistitException {
        delegate.writeRow(session, rowData);
    }

    public void writeRowForBulkLoad(Session session,
                                    Exchange hEx,
                                    RowDef rowDef,
                                    RowData rowData,
                                    int[] ordinals,
                                    int[] nKeyColumns,
                                    FieldDef[] hKeyFieldDefs,
                                    Object[] hKeyValues) throws PersistitException {
        delegate.writeRowForBulkLoad(session, hEx, rowDef, rowData, ordinals, nKeyColumns, hKeyFieldDefs, hKeyValues);
    }

    public void updateTableStats(Session session, RowDef rowDef, long rowCount) {
        delegate.updateTableStats(session, rowDef, rowCount);
    }

    public void deleteRow(Session session, RowData rowData) throws PersistitException {
        delegate.deleteRow(session, rowData);
    }

    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector, Index[] indexes) throws PersistitException {
        delegate.updateRow(session, oldRowData, newRowData, columnSelector, indexes);
    }

    public void dropGroup(Session session, int rowDefId) throws PersistitException {
        delegate.dropGroup(session, rowDefId);
    }

    public void truncateGroup(Session session, int rowDefId) throws PersistitException {
        delegate.truncateGroup(session, rowDefId);
    }

    public void truncateTableStatus(Session session, int rowDefId) throws PersistitException {
        delegate.truncateTableStatus(session, rowDefId);
    }

    public RowCollector getSavedRowCollector(Session session, int tableId) throws InvalidOperationException {
        return delegate.getSavedRowCollector(session, tableId);
    }

    public void addSavedRowCollector(Session session, RowCollector rc) {
        delegate.addSavedRowCollector(session, rc);
    }

    public void removeSavedRowCollector(Session session, RowCollector rc) throws InvalidOperationException {
        delegate.removeSavedRowCollector(session, rc);
    }

    @SuppressWarnings("deprecation")
    public RowCollector newRowCollector(Session session, int rowDefId, int indexId, int scanFlags, RowData start, RowData end, byte[] columnBitMap, ScanLimit scanLimit) {
        return delegate.newRowCollector(session, rowDefId, indexId, scanFlags, start, end, columnBitMap, scanLimit);
    }

    public RowCollector newRowCollector(Session session, int scanFlags, int rowDefId, int indexId, byte[] columnBitMap, RowData start, ColumnSelector startColumns, RowData end, ColumnSelector endColumns, ScanLimit scanLimit) {
        return delegate.newRowCollector(session, scanFlags, rowDefId, indexId, columnBitMap, start, startColumns, end, endColumns, scanLimit);
    }

    public long getRowCount(Session session, boolean exact, RowData start, RowData end, byte[] columnBitMap) {
        return delegate.getRowCount(session, exact, start, end, columnBitMap);
    }

    public TableStatistics getTableStatistics(Session session, int tableId) {
        return delegate.getTableStatistics(session, tableId);
    }

    public void flushIndexes(Session session) {
        delegate.flushIndexes(session);
    }

    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
        delegate.buildIndexes(session, indexes, defer);
    }

    public void deleteIndexes(Session session, Collection<? extends Index> indexes) {
        delegate.deleteIndexes(session, indexes);
    }

    public void deleteSequences (Session session, Collection<? extends Sequence> sequences) {
        delegate.deleteSequences(session, sequences);
    }
    
    public void removeTrees(Session session, Table table) {
        delegate.removeTrees(session, table);
    }

    @Override
    public void buildAllIndexes(Session session, boolean deferIndexes) {
        delegate.buildAllIndexes(session, deferIndexes);
    }

    public boolean isDeferIndexes() {
        return delegate.isDeferIndexes();
    }

    public void setDeferIndexes(boolean defer) {
        delegate.setDeferIndexes(defer);
    }

    @Override
    public void truncateIndex(Session session, Collection<? extends Index> indexes) {
        delegate.truncateIndex(session, indexes);
    }
}
