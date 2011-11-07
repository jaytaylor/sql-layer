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

import com.akiban.server.aggregation.AggregatorRegistry;
import com.akiban.server.error.InvalidPortException;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.expression.ExpressionRegistry;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.instrumentation.InstrumentationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.google.inject.Inject;

/** The PostgreSQL server service.
 * @see PostgresServer
*/
public class PostgresServerManager implements PostgresService, Service<PostgresService>, JmxManageable {
    private PostgresServer server = null;
    private final PostgresServiceRequirements reqs;

    @Inject
    public PostgresServerManager(ConfigurationService config,
                                 DXLService dxlService,
                                 InstrumentationService instrumentation,
                                 SessionService sessionService,
                                 Store store,
                                 TreeService treeService,
                                 ExpressionRegistry expressionRegistry,
                                 AggregatorRegistry aggregatorRegistry
    ) {
        this.reqs = new PostgresServiceRequirements(dxlService, instrumentation, sessionService, store, treeService,
                expressionRegistry, aggregatorRegistry, config);
    }

    /*** Service<PostgresService> ***/

    public PostgresService cast() {
        return this;
    }

    public Class<PostgresService> castClass() {
        return PostgresService.class;
    }

    public void start() throws ServiceStartupException {
        String portString = reqs.config().getProperty("akserver.postgres.port");
        int port = Integer.parseInt(portString);
        String capacityString = reqs.config().getProperty("akserver.postgres.statementCacheCapacity");
        int statementCacheCapacity = Integer.parseInt(capacityString);

        if (port > 0) {
            server = new PostgresServer(port, statementCacheCapacity, reqs);
            server.start();
        } else {
            throw new InvalidPortException(port);
        }
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
