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

package com.akiban.server.service.dxl;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.ServiceNotStartedException;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.listener.ListenerService;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.t3expressions.T3RegistryService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DXLServiceImpl implements DXLService, Service, JmxManageable {
    private final static String CONFIG_USE_GLOBAL_LOCK = "akserver.dxl.use_global_lock";
    private final static Logger LOG = LoggerFactory.getLogger(DXLServiceImpl.class);

    private final Object MONITOR = new Object();

    private volatile HookableDDLFunctions ddlFunctions;
    private volatile DMLFunctions dmlFunctions;
    private final SchemaManager schemaManager;
    private final Store store;
    private final SessionService sessionService;
    private final IndexStatisticsService indexStatisticsService;
    private final ConfigurationService configService;
    private final T3RegistryService t3Registry;
    private final TransactionService txnService;
    private final LockService lockService;
    private final ListenerService listenerService;

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("DXL", new DXLMXBeanImpl(this, sessionService), DXLMXBean.class);
    }

    @Override
    public void start() {
        boolean useGlobalLock = Boolean.parseBoolean(configService.getProperty(CONFIG_USE_GLOBAL_LOCK));
        DXLReadWriteLockHook.only().setDDLLockEnabled(useGlobalLock);
        LOG.debug("Using global DDL lock: {}", useGlobalLock);
        List<DXLFunctionsHook> hooks = getHooks(useGlobalLock);
        BasicDXLMiddleman middleman = BasicDXLMiddleman.create();
        HookableDDLFunctions localDdlFunctions
                = new HookableDDLFunctions(createDDLFunctions(middleman), hooks,sessionService);
        DMLFunctions localDmlFunctions = new HookableDMLFunctions(createDMLFunctions(middleman, localDdlFunctions),
                hooks, sessionService);
        synchronized (MONITOR) {
            if (ddlFunctions != null) {
                throw new ServiceStartupException("service already started");
            }
            ddlFunctions = localDdlFunctions;
            dmlFunctions = localDmlFunctions;
        }
    }

    DMLFunctions createDMLFunctions(BasicDXLMiddleman middleman, DDLFunctions newlyCreatedDDLF) {
        return new BasicDMLFunctions(middleman, schemaManager, store, newlyCreatedDDLF,
                                     indexStatisticsService, listenerService);
    }

    DDLFunctions createDDLFunctions(BasicDXLMiddleman middleman) {
        return new BasicDDLFunctions(middleman, schemaManager, store, indexStatisticsService,
                                     t3Registry, lockService, txnService, listenerService);
    }

    @Override
    public void stop() {
        synchronized (MONITOR) {
            if (ddlFunctions == null) {
                throw new ServiceNotStartedException("DDL Functions stop");
            }
            ddlFunctions = null;
            dmlFunctions = null;
            BasicDXLMiddleman.destroy();
        }
    }

    @Override
    public DDLFunctions ddlFunctions() {
        final DDLFunctions ret = ddlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DDL Functions");
        }
        return ret;
    }

    @Override
    public DMLFunctions dmlFunctions() {
        final DMLFunctions ret = dmlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DML Functions");
        }
        return ret;
    }

    protected List<DXLFunctionsHook> getHooks(boolean useGlobalLock) {
        List<DXLFunctionsHook> hooks = new ArrayList<>();
        if(useGlobalLock) {
            LOG.warn("Global DDL lock is enabled");
            hooks.add(DXLReadWriteLockHook.only());
        }
        hooks.add(new DXLTransactionHook(txnService));
        return hooks;
    }

    @Override
    public void crash() {
        BasicDXLMiddleman.destroy();
    }

    @Inject
    public DXLServiceImpl(SchemaManager schemaManager, Store store, SessionService sessionService,
                          IndexStatisticsService indexStatisticsService, ConfigurationService configService,
                          T3RegistryService t3Registry, TransactionService txnService, LockService lockService,
                          ListenerService listenerService) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.sessionService = sessionService;
        this.indexStatisticsService = indexStatisticsService;
        this.configService = configService;
        this.t3Registry = t3Registry;
        this.txnService = txnService;
        this.lockService = lockService;
        this.listenerService = listenerService;
    }

    // for use by subclasses

    protected final SchemaManager schemaManager() {
        return schemaManager;
    }

    protected final Store store() {
        return store;
    }

    protected final IndexStatisticsService indexStatisticsService() {
        return indexStatisticsService;
    }

    protected final T3RegistryService t3Registry() {
        return t3Registry;
    }

    protected final TransactionService txnService() {
        return txnService;
    }

    protected final LockService lockService() {
        return lockService;
    }

    protected final ListenerService listenerService() {
        return listenerService;
    }

    protected final Session session() {
        return null;
    }
}
