
package com.akiban.sql.server;

import com.akiban.server.AkServerInterface;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.externaldata.ExternalDataService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.monitor.MonitorService;
import com.akiban.server.service.routines.RoutineLoader;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.t3expressions.T3RegistryService;

public final class ServerServiceRequirements {

    public ServerServiceRequirements(AkServerInterface akServer,
                                     DXLService dxlService,
                                     MonitorService monitor,
                                     SessionService sessionService,
                                     Store store,
                                     TreeService treeService,
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
        this.treeService = treeService;
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

    public TreeService treeService() {
        return treeService;
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
    private final TreeService treeService;
    private final FunctionsRegistry functionsRegistry;
    private final ConfigurationService config;
    private final IndexStatisticsService indexStatistics;
    private final T3RegistryService t3RegistryService;
    private final RoutineLoader routineLoader;
    private final TransactionService txnService;
    private final SecurityService securityService;
    private final ServiceManager serviceManager;
}
