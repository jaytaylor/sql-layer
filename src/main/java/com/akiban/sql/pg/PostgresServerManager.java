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

package com.akiban.sql.pg;

import com.akiban.sql.server.ServerServiceRequirements;

import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.instrumentation.InstrumentationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;

import com.google.inject.Inject;

/** The PostgreSQL server service.
 * @see PostgresServer
*/
public class PostgresServerManager implements PostgresService, Service<PostgresService>, JmxManageable {
    private final ServerServiceRequirements reqs;
    private PostgresServer server = null;

    @Inject
    public PostgresServerManager(ConfigurationService config,
                                 DXLService dxlService,
                                 InstrumentationService instrumentation,
                                 SessionService sessionService,
                                 Store store,
                                 TreeService treeService,
                                 FunctionsRegistry functionsRegistry,
                                 IndexStatisticsService indexStatisticsService) {
        reqs = new ServerServiceRequirements(dxlService, instrumentation, sessionService, store, treeService, functionsRegistry, config, indexStatisticsService);
    }

    /*** Service<PostgresService> ***/

    public PostgresService cast() {
        return this;
    }

    public Class<PostgresService> castClass() {
        return PostgresService.class;
    }

    public void start() throws ServiceStartupException {
        server = new PostgresServer(reqs);
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    public void crash() {
        stop();
    }

    /*** PostgresService ***/

    public int getPort() {
        return server.getPort();
    }
    
    public PostgresServer getServer() {
        return server;
    }

    /*** JmxManageable ***/
    
    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("PostgresServer", server, PostgresMXBean.class);
    }
}
