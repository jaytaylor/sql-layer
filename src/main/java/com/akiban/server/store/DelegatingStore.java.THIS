/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server.store;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.persistit.Key;

import java.util.Collection;

public abstract class DelegatingStore<S extends Store & Service> implements Store, Service {

    private final S delegate;

    public DelegatingStore(S delegate) {
        this.delegate = delegate;
    }

    // TODO: Ditch DelegatingStore altogether
    public S getDelegate() {
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

    @Override
    public AkibanInformationSchema getAIS(Session session) {
        return delegate.getAIS(session);
    }

    @Override
    public RowDef getRowDef(Session session, int rowDefID) {
        return delegate.getRowDef(session, rowDefID);
    }

    @Override
    public RowDef getRowDef(Session session, TableName tableName) {
        return delegate.getRowDef(session, tableName);
    }

    public void writeRow(Session session, RowData rowData) {
        delegate.writeRow(session, rowData);
    }

    public void deleteRow(Session session, RowData rowData, boolean deleteIndexes, boolean cascadeDelete) {
        delegate.deleteRow(session, rowData, deleteIndexes, cascadeDelete);
    }

    public void updateRow(Session session, RowData oldRowData, RowData newRowData, ColumnSelector columnSelector, Index[] indexes) {
        delegate.updateRow(session, oldRowData, newRowData, columnSelector, indexes);
    }
    
    public long nextSequenceValue(Session session, Sequence sequence) throws Exception {
        return delegate.nextSequenceValue(session, sequence);
    }
    
    public long curSequenceValue(Session session, Sequence sequence) throws Exception {
        return delegate.curSequenceValue(session, sequence);
    }

    public void dropGroup(Session session, Group group) {
        delegate.dropGroup(session, group);
    }

    public void truncateGroup(Session session, Group group) {
        delegate.truncateGroup(session, group);
    }

    public void truncateTableStatus(Session session, int rowDefId) {
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

    public RowCollector newRowCollector(Session session, int scanFlags, int rowDefId, int indexId, byte[] columnBitMap, RowData start, ColumnSelector startColumns, RowData end, ColumnSelector endColumns, ScanLimit scanLimit) {
        return delegate.newRowCollector(session, scanFlags, rowDefId, indexId, columnBitMap, start, startColumns, end, endColumns, scanLimit);
    }

    public long getRowCount(Session session, boolean exact, RowData start, RowData end, byte[] columnBitMap) {
        return delegate.getRowCount(session, exact, start, end, columnBitMap);
    }

    public TableStatistics getTableStatistics(Session session, int tableId) {
        return delegate.getTableStatistics(session, tableId);
    }

    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
        delegate.buildIndexes(session, indexes, defer);
    }

    public void deleteIndexes(Session session, Collection<? extends Index> indexes) {
        delegate.deleteIndexes(session, indexes);
    }

    @Override
    public void removeTrees(Session session, UserTable table) {
        delegate.removeTrees(session, table);
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        delegate.truncateIndexes(session, indexes);
    }

    @Override
    public void removeTrees(Session session, Collection<? extends TreeLink> treeLinks) {
        delegate.removeTrees(session, treeLinks);
    }

    @Override
    public StoreAdapter createAdapter(Session session, Schema schema) {
        return delegate.createAdapter(session, schema);
    }

    @Override
    public void setIndexStatistics(IndexStatisticsService indexStatistics) {
        delegate.setIndexStatistics(indexStatistics);
    }

    @Override
    public void truncateTree(Session session, TreeLink treeLink) {
        delegate.truncateTree(session, treeLink);
    }

    @Override
    public Key createKey() {
        return delegate.createKey();
    }

    @Override
    public void deleteSequences(Session session, Collection<? extends Sequence> sequences) {
        delegate.deleteSequences(session, sequences);
    }

    @Override
    public void removeTree(Session session, TreeLink treeLink) {
        delegate.removeTree(session, treeLink);
    }
}
