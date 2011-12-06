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
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.DelegatingStore;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

import com.google.inject.Inject;

import com.persistit.exception.PersistitException;

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
        // The default implementation is an OperatorStore, but some
        // tests have the PersistitStore directly. We don't need the
        // OperatorStore, but do need methods not on Store.
        // TODO: Seems like a mess. Cf. PostgresServerConnection.updateAIS().
        while (!(store instanceof PersistitStore))
            store = ((DelegatingStore<Store>)store).getDelegate();
        this.store = (PersistitStore)store;
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
            throw new PersistItErrorException(ex);
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
        for (Index index : indexes) {
            try {
                IndexStatistics indexStatistics = 
                    storeStats.computeIndexStatistics(session, index);
                if (indexStatistics != null) {
                    storeStats.storeIndexStatistics(session, indexStatistics);
                    cache.put(index, indexStatistics);
                }
            }
            catch (PersistitException ex) {
                throw new PersistItErrorException(ex);
            }
        }
    }

    @Override
    public void deleteIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        for (Index index : indexes) {
            try {
                storeStats.deleteIndexStatistics(session, index);
            }
            catch (PersistitException ex) {
                throw new PersistItErrorException(ex);
            }
        }
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
                List<Index> indexes = new ArrayList<Index>();
                Set<Group> groups = new HashSet<Group>();
                AkibanInformationSchema ais = schemaManager.getAis(session);
                for (UserTable table : ais.getUserTables().values()) {
                    if (table.getName().getSchemaName().equals(schema))
                        indexes.addAll(table.getIndexes());
                    if (groups.add(table.getGroup()))
                        indexes.addAll(table.getGroup().getIndexes());
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
                File file = new File(toFile);
                new IndexStatisticsYamlLoader(ais, schema).dump(toDump, file);
                return file.getAbsolutePath();
            }
            finally {
                session.close();
            }
        }
    }

}
