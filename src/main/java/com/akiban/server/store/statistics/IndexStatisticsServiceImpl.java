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

package com.akiban.server.store.statistics;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.Service;
import com.akiban.server.service.dxl.DXLTransactionHook;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

import com.google.inject.Inject;

import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

import java.util.*;
import java.io.File;
import java.io.IOException;

public class IndexStatisticsServiceImpl implements IndexStatisticsService, Service<IndexStatisticsService>, JmxManageable
{
    private final PersistitStore store;
    private final TreeService treeService;
    // Following couple only used by JMX method, where there is no context.
    private final SchemaManager schemaManager;
    private final SessionService sessionService;

    private PersistitStoreIndexStatistics storeStats;
    private Map<Index,IndexStatistics> cache;

    @Inject
    public IndexStatisticsServiceImpl(Store store, TreeService treeService,
                                      SchemaManager schemaManager, SessionService sessionService) {
        this.store = store.getPersistitStore();
        this.treeService = treeService;
        this.schemaManager = schemaManager;
        this.sessionService = sessionService;
    }
    
    /* Service */

    @Override
    public IndexStatisticsService cast() {
        return this;
    }

    @Override
    public Class<IndexStatisticsService> castClass() {
        return IndexStatisticsService.class;
    }

    @Override
    public void start() {
        cache = Collections.synchronizedMap(new WeakHashMap<Index,IndexStatistics>());
        storeStats = new PersistitStoreIndexStatistics(store, treeService);
    }

    @Override
    public void stop() {
        cache = null;
        storeStats = null;
    }

    @Override
    public void crash() {
        stop();
    }

    /* IndexStatisticsService */

    @Override
    public long countEntries(Session session, Index index) throws PersistitInterruptedException {
        if (index.isTableIndex()) {
            return store.getTableStatus(((TableIndex)index).getTable()).getRowCount();
        }
        final Exchange ex = store.getExchange(session, index);
        try {
            return AccumulatorAdapter.getSnapshot(AccumulatorAdapter.AccumInfo.ROW_COUNT, treeService, ex.getTree());
        }
        finally {
            store.releaseExchange(session, ex);
        }
    }

    @Override
    public long countEntriesApproximate(Session session, Index index) {
        if (index.isTableIndex()) {
            return store.getTableStatus(((TableIndex)index).getTable()).getApproximateRowCount();
        }
        final Exchange ex = store.getExchange(session, index);
        try {
            return AccumulatorAdapter.getLiveValue(AccumulatorAdapter.AccumInfo.ROW_COUNT, treeService, ex.getTree());
        }
        finally {
            store.releaseExchange(session, ex);
        }
    }

    @Override
    public IndexStatistics getIndexStatistics(Session session, Index index) {
        // TODO: Use getAnalysisTimestamp() of -1 to mark an "empty"
        // analysis to save going to disk for the same index every
        // time. Should this be a part of the IndexStatistics contract
        // somehow?
        IndexStatistics result = cache.get(index);
        if (result != null) {
            if (result.getAnalysisTimestamp() < 0)
                return null;
            else
                return result;
        }
        try {
            result = storeStats.loadIndexStatistics(session, index);
        }
        catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
        if (result != null)
            return result;
        result = new IndexStatistics(index);
        result.setAnalysisTimestamp(-1);
        cache.put(index, result);
        return null;
    }

    @Override
    public void updateIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        final Map<Index,IndexStatistics> updates = new HashMap<Index, IndexStatistics>(indexes.size());
        for (Index index : indexes) {
            try {
                IndexStatistics indexStatistics = 
                    storeStats.computeIndexStatistics(session, index);
                if (indexStatistics != null) {
                    storeStats.storeIndexStatistics(session, indexStatistics);
                    updates.put(index, indexStatistics);
                }
            }
            catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
            }
        }
        DXLTransactionHook.addCommitSuccessCallback(session, new Runnable() {
            @Override
            public void run() {
                cache.putAll(updates);
            }
        });
    }

    @Override
    public void deleteIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        for (Index index : indexes) {
            try {
                storeStats.deleteIndexStatistics(session, index);
            }
            catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
            }
        }
    }

    @Override
    public void loadIndexStatistics(Session session, 
                                    String schema, File file) throws IOException {
        AkibanInformationSchema ais = schemaManager.getAis(session);
        Map<Index,IndexStatistics> stats = 
            new IndexStatisticsYamlLoader(ais, schema).load(file);
        for (Map.Entry<Index,IndexStatistics> entry : stats.entrySet()) {
            Index index = entry.getKey();
            IndexStatistics indexStatistics = entry.getValue();
            try {
                storeStats.storeIndexStatistics(session, indexStatistics);
            }
            catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
            }
            cache.put(index, indexStatistics);
        }
    }

    @Override
    public void dumpIndexStatistics(Session session, 
                                    String schema, File file) throws IOException {
        List<Index> indexes = new ArrayList<Index>();
        Set<Group> groups = new HashSet<Group>();
        AkibanInformationSchema ais = schemaManager.getAis(session);
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getName().getSchemaName().equals(schema)) {
                indexes.addAll(table.getIndexes());
                if (groups.add(table.getGroup()))
                    indexes.addAll(table.getGroup().getIndexes());
            }
        }
        // Get all the stats already computed for an index on this schema.
        List<IndexStatistics> toDump = new ArrayList<IndexStatistics>();
        for (Index index : indexes) {
            IndexStatistics stats = getIndexStatistics(session, index);
            if (stats != null) {
                toDump.add(stats);
            }
        }
        Collections.sort(toDump, new Comparator<IndexStatistics>() {
                             @Override
                             public int compare(IndexStatistics i1, IndexStatistics i2) {
                                 return i1.getIndex().getIndexName().toString()
                                     .compareTo(i2.getIndex().getIndexName().toString());
                             }
                         });
        new IndexStatisticsYamlLoader(ais, schema).dump(toDump, file);
    }

    /* JmxManageable */

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("IndexStatistics", 
                                 new JmxBean(), 
                                 IndexStatisticsMXBean.class);
    }
    
    class JmxBean implements IndexStatisticsMXBean {
        @Override
        public String dumpIndexStatistics(String schema, String toFile) 
                throws IOException {
            Session session = sessionService.createSession();
            try {
                File file = new File(toFile);
                IndexStatisticsServiceImpl.this.dumpIndexStatistics(session, schema, file);
                return file.getAbsolutePath();
            }
            finally {
                session.close();
            }
        }
    }
}
