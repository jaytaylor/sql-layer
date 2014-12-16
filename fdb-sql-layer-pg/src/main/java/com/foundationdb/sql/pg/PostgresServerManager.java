/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.pg;

import com.foundationdb.server.error.ServiceAlreadyStartedException;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.sql.optimizer.rule.cost.CostModelFactory;
import com.foundationdb.sql.server.ServerServiceRequirements;

import com.google.inject.Inject;

/** The PostgreSQL server service.
 * @see PostgresServer
*/
public class PostgresServerManager implements PostgresService, Service, JmxManageable {
    private final ServerServiceRequirements reqs;
    private final BasicInfoSchemaTablesService infoSchemaService;
    private PostgresServer server = null;

    @Inject
    public PostgresServerManager(LayerInfoInterface layerInfo,
                                 DXLService dxlService,
                                 MonitorService monitor,
                                 SessionService sessionService,
                                 Store store,
                                 ConfigurationService config,
                                 IndexStatisticsService indexStatisticsService,
                                 TypesRegistryService overloadResolutionService,
                                 RoutineLoader routineLoader,
                                 TransactionService txnService,
                                 SecurityService securityService,
                                 CostModelFactory costModel,
                                 MetricsService metricsService,
                                 ServiceManager serviceManager,
                                 BasicInfoSchemaTablesService infoSchemaService) {
        reqs = new ServerServiceRequirements(layerInfo, dxlService, monitor,
                sessionService, store,
                config, indexStatisticsService, overloadResolutionService, 
                routineLoader, txnService, securityService, costModel,
                metricsService,
                serviceManager);
        this.infoSchemaService = infoSchemaService;
    }

    @Override
    public void start() throws ServiceAlreadyStartedException {
        infoSchemaService.setPostgresTypeMapper(new BasicInfoSchemaTablesService.PostgresTypeMapper() {
                @Override
                public long getOid(TInstance type) {
                    return PostgresType.fromTInstance(type).getOid();
                }
            });
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
    public String getHost() {
        return server.getHost();
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
