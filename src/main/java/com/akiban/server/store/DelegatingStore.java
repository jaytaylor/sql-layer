
package com.akiban.server.store;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
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

    public RowDef getRowDef(Session session, int rowDefID) {
        return delegate.getRowDef(session, rowDefID);
    }

    @Override
    public void startBulkLoad(Session session) {
        delegate.startBulkLoad(session);
    }

    @Override
    public boolean isBulkloading() {
        return delegate.isBulkloading();
    }

    @Override
    public void finishBulkLoad(Session session) {
        delegate.finishBulkLoad(session);
    }

    public void writeRow(Session session, RowData rowData) throws PersistitException {
        delegate.writeRow(session, rowData);
    }

    public void deleteRow(Session session, RowData rowData, boolean deleteIndexes, boolean cascadeDelete) throws PersistitException {
        delegate.deleteRow(session, rowData, deleteIndexes, cascadeDelete);
    }

    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector, Index[] indexes) throws PersistitException {
        delegate.updateRow(session, oldRowData, newRowData, columnSelector, indexes);
    }

    public void dropGroup(Session session, Group group) {
        delegate.dropGroup(session, group);
    }

    public void truncateGroup(Session session, Group group) throws PersistitException {
        delegate.truncateGroup(session, group);
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

    public boolean isDeferIndexes() {
        return delegate.isDeferIndexes();
    }

    public void setDeferIndexes(boolean defer) {
        delegate.setDeferIndexes(defer);
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        delegate.truncateIndexes(session, indexes);
    }

    @Override
    public void removeTrees(Session session, Collection<? extends TreeLink> treeLinks) {
        delegate.removeTrees(session, treeLinks);
    }
}
