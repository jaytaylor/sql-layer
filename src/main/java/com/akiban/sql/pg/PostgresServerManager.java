
package com.akiban.sql.pg;

import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.server.ServerServiceRequirements;

import com.akiban.server.AkServerInterface;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.monitor.MonitorService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.routines.RoutineLoader;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;

import com.google.inject.Inject;

/** The PostgreSQL server service.
 * @see PostgresServer
*/
public class PostgresServerManager implements PostgresService, Service, JmxManageable {
    private final ServerServiceRequirements reqs;
    private PostgresServer server = null;

    @Inject
    public PostgresServerManager(AkServerInterface akServer,
                                 DXLService dxlService,
                                 MonitorService monitor,
                                 SessionService sessionService,
                                 Store store,
                                 TreeService treeService,
                                 FunctionsRegistry functionsRegistry,
                                 ConfigurationService config,
                                 IndexStatisticsService indexStatisticsService,
                                 T3RegistryService overloadResolutionService,
                                 RoutineLoader routineLoader,
                                 TransactionService txnService,
                                 SecurityService securityService,
                                 ServiceManager serviceManager) {
        reqs = new ServerServiceRequirements(akServer, dxlService, monitor, 
                sessionService, store, treeService, functionsRegistry, 
                config, indexStatisticsService, overloadResolutionService, 
                routineLoader, txnService, securityService, serviceManager);
    }

    @Override
    public void start() throws ServiceStartupException {
        server = new PostgresServer(reqs);
        server.start();
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Override
    public void crash() {
        stop();
    }

    /*** PostgresService ***/

    @Override
    public int getPort() {
        return server.getPort();
    }
    
    @Override
    public PostgresServer getServer() {
        return server;
    }

    /*** JmxManageable ***/
    
    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("PostgresServer", server, PostgresMXBean.class);
    }
}
