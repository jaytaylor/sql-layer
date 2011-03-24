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

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.RowDef;
import com.akiban.server.RowDefCache;
import com.akiban.server.TableStatus;
import com.akiban.server.TableStatusAccumulator;
import com.akiban.server.service.AfterStart;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.persistit.Persistit;
import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.TimestampAllocator.CheckpointListener;

public class PersistitStoreTableStatusService implements
        Service<TableStatusService>, TableStatusService, AfterStart,
        CheckpointListener {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistitStoreTableStatusService.class.getName());

    private ServiceManager serviceManager;
    private Checkpoint lastCommittedCheckpoint = new Checkpoint(0, 0);

    @Override
    public void start() throws Exception {
        serviceManager = ServiceManagerImpl.get();
        final Session session = new SessionImpl();
        serviceManager.getSchemaManager().loadTableStatusRecords(session);
    }

    @Override
    public void afterStart() throws Exception {
        final long timestamp = serviceManager.getSchemaManager()
                .getUpdateTimestamp();
        final Collection<TableStatusAccumulator> accumulators = serviceManager
                .getTreeService().getAccumulators();
        final Map<Integer, TableStatus> tableStatusMap = serviceManager
                .getStore().getRowDefCache().getTableStatusMap();
        for (final TableStatusAccumulator tsa : accumulators) {
            TableStatus tableStatus = tableStatusMap.get(tsa.getTableId());
            if (tableStatus != null) {
                tableStatus.incrementRowCount(serviceManager.getTreeService()
                        .getDb().getCurrentCheckpoint(), timestamp,
                        tsa.getDeltaRowCount());
            }
        }
        serviceManager.getTreeService().getDb().addCheckpointListener(this);
    }

    @Override
    public void stop() throws Exception {
        serviceManager.getTreeService().getDb().removeCheckpointListener(this);
    }

    @Override
    public void crash() throws Exception {
        stop();
    }

    @Override
    public TableStatusService cast() {
        return this;
    }

    @Override
    public Class castClass() {
        return TableStatusService.class;
    }

    @Override
    public boolean save(Checkpoint checkpoint) {
        if (checkpoint == lastCommittedCheckpoint) {
            return true;
        }
        try {
            final Persistit db = serviceManager.getTreeService().getDb();
            final long timestamp = checkpoint.getTimestamp();
            if (db.pendingTransactionCount(timestamp) > 0) {
                return false;
            }
            final Session session = new SessionImpl();
            serviceManager.getSchemaManager().saveTableStatusRecords(session,
                    timestamp);
            lastCommittedCheckpoint = checkpoint;
        } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Failed to save TableStatus records", e);
            }
            return false;
        }
        return true;
    }

    @Override
    public void done(Checkpoint checkpoint) {
        try {
            final RowDefCache rowDefCache = serviceManager.getStore()
                    .getRowDefCache();
            for (final RowDef rowDef : rowDefCache.getRowDefs()) {
                TableStatus ts = rowDef.getTableStatus();
                ts.pruneObsoleteVersions(checkpoint.getTimestamp());
            }
        } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Failed to save TableStatus records", e);
            }
        }
    }
}
