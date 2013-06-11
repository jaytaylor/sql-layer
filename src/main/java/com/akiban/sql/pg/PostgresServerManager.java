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
                                 FunctionsRegistry functionsRegistry,
                                 ConfigurationService config,
                                 IndexStatisticsService indexStatisticsService,
                                 T3RegistryService overloadResolutionService,
                                 RoutineLoader routineLoader,
                                 TransactionService txnService,
                                 SecurityService securityService,
                                 ServiceManager serviceManager) {
        reqs = new ServerServiceRequirements(akServer, dxlService, monitor, 
                sessionService, store, functionsRegistry,
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
