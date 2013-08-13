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

package com.foundationdb.sql.server;

import com.foundationdb.server.AkServerInterface;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.externaldata.ExternalDataService;
import com.foundationdb.server.service.functions.FunctionsRegistry;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.t3expressions.T3RegistryService;

public final class ServerServiceRequirements {

    public ServerServiceRequirements(AkServerInterface akServer,
                                     DXLService dxlService,
                                     MonitorService monitor,
                                     SessionService sessionService,
                                     Store store,
                                     FunctionsRegistry functionsRegistry,
                                     ConfigurationService config,
                                     IndexStatisticsService indexStatistics,
                                     T3RegistryService t3RegistryService,
                                     RoutineLoader routineLoader,
                                     TransactionService txnService,
                                     SecurityService securityService,
                                     ServiceManager serviceManager) {
        this.akServer = akServer;
        this.dxlService = dxlService;
        this.monitor = monitor;
        this.sessionService = sessionService;
        this.store = store;
        this.functionsRegistry = functionsRegistry;
        this.config = config;
        this.indexStatistics = indexStatistics;
        this.t3RegistryService = t3RegistryService;
        this.routineLoader = routineLoader;
        this.txnService = txnService;
        this.securityService = securityService;
        this.serviceManager = serviceManager;
    }

    public AkServerInterface akServer() {
        return akServer;
    }

    public DXLService dxl() {
        return dxlService;
    }

    public MonitorService monitor() {
        return monitor;
    }

    public SessionService sessionService() {
        return sessionService;
    }

    public Store store() {
        return store;
    }

    public FunctionsRegistry functionsRegistry() {
        return functionsRegistry;
    }

    public T3RegistryService t3RegistryService() {
        return t3RegistryService;
    }

    public ConfigurationService config() {
        return config;
    }

    public IndexStatisticsService indexStatistics() {
        return indexStatistics;
    }

    public RoutineLoader routineLoader() {
        return routineLoader;
    }

    public TransactionService txnService() {
        return txnService;
    }

    public ServiceManager serviceManager() {
        return serviceManager;
    }

    public SecurityService securityService() {
        return securityService;
    }

    /* Less commonly used, started on demand */

    public ExternalDataService externalData() {
        return serviceManager.getServiceByClass(ExternalDataService.class);
    }

    private final AkServerInterface akServer;
    private final DXLService dxlService;
    private final MonitorService monitor;
    private final SessionService sessionService;
    private final Store store;
    private final FunctionsRegistry functionsRegistry;
    private final ConfigurationService config;
    private final IndexStatisticsService indexStatistics;
    private final T3RegistryService t3RegistryService;
    private final RoutineLoader routineLoader;
    private final TransactionService txnService;
    private final SecurityService securityService;
    private final ServiceManager serviceManager;
}
