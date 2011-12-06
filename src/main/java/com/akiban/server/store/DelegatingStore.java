/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.store;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

import java.util.Collection;

public class DelegatingStore<S extends Store> implements Store {

    private final S delegate;

    public DelegatingStore(S delegate) {
        this.delegate = delegate;
    }

    public S getDelegate() {
        return delegate;
    }

    // Store interface -- non-delegating

    @Override
    public Store cast() {
        return this;
    }

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

    public Class<Store> castClass() {
        return delegate.castClass();
    }

    public RowDefCache getRowDefCache() {
        return delegate.getRowDefCache();
    }

    public void writeRow(final Session session, final RowData rowData) throws PersistitException {
        delegate.writeRow(session, rowData);
    }

    public void writeRowForBulkLoad(final Session session, Exchange hEx, RowDef rowDef, RowData rowData, int[] ordinals, int[] nKeyColumns, FieldDef[] hKeyFieldDefs, Object[] hKeyValues) throws PersistitException {
        delegate.writeRowForBulkLoad(session, hEx, rowDef, rowData, ordinals, nKeyColumns, hKeyFieldDefs, hKeyValues);
    }

    public void updateTableStats(final Session session, RowDef rowDef, long rowCount) {
        delegate.updateTableStats(session, rowDef, rowCount);
    }

    public void deleteRow(final Session session, final RowData rowData) throws PersistitException {
        delegate.deleteRow(session, rowData);
    }

    public void updateRow(final Session session, final RowData oldRowData, final RowData newRowData, final ColumnSelector columnSelector) throws PersistitException {
        delegate.updateRow(session, oldRowData, newRowData, columnSelector);
    }

    public void truncateGroup(final Session session, final int rowDefId) throws PersistitException {
        delegate.truncateGroup(session, rowDefId);
    }

    public void truncateTableStatus(final Session session, final int rowDefId) throws PersistitException {
        delegate.truncateTableStatus(session, rowDefId);
    }

    public RowCollector getSavedRowCollector(final Session session, final int tableId) throws InvalidOperationException {
        return delegate.getSavedRowCollector(session, tableId);
    }

    public void addSavedRowCollector(final Session session, final RowCollector rc) {
        delegate.addSavedRowCollector(session, rc);
    }

    public void removeSavedRowCollector(final Session session, final RowCollector rc) throws InvalidOperationException {
        delegate.removeSavedRowCollector(session, rc);
    }

    public RowCollector newRowCollector(Session session, int rowDefId, int indexId, int scanFlags, RowData start, RowData end, byte[] columnBitMap, ScanLimit scanLimit) {
        return delegate.newRowCollector(session, rowDefId, indexId, scanFlags, start, end, columnBitMap, scanLimit);
    }

    public RowCollector newRowCollector(Session session, int scanFlags, int rowDefId, int indexId, byte[] columnBitMap, RowData start, ColumnSelector startColumns, RowData end, ColumnSelector endColumns, ScanLimit scanLimit) {
        return delegate.newRowCollector(session, scanFlags, rowDefId, indexId, columnBitMap, start, startColumns, end, endColumns, scanLimit);
    }

    public long getRowCount(final Session session, final boolean exact, final RowData start, final RowData end, final byte[] columnBitMap) {
        return delegate.getRowCount(session, exact, start, end, columnBitMap);
    }

    public TableStatistics getTableStatistics(final Session session, int tableId) {
        return delegate.getTableStatistics(session, tableId);
    }

    public void analyzeTable(final Session session, final int tableId) {
        delegate.analyzeTable(session, tableId);
    }

    public void analyzeTable(final Session session, final int tableId, final int sampleSize) {
        delegate.analyzeTable(session, tableId, sampleSize);
    }

    public void flushIndexes(final Session session) {
        delegate.flushIndexes(session);
    }

    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
        delegate.buildIndexes(session, indexes, defer);
    }

    public void deleteIndexes(Session session, Collection<? extends Index> indexes) {
        delegate.deleteIndexes(session, indexes);
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

    public void setDeferIndexes(final boolean defer) {
        delegate.setDeferIndexes(defer);
    }
}
