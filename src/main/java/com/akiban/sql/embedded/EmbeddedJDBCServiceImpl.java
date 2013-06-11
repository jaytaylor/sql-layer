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

package com.akiban.sql.embedded;

import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.server.ServerServiceRequirements;

import com.akiban.server.AkServerInterface;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.monitor.MonitorService;
import com.akiban.server.service.routines.RoutineLoader;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.security.User;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;

import java.security.Principal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class EmbeddedJDBCServiceImpl implements EmbeddedJDBCService, Service {
    private final ServerServiceRequirements reqs;
    private JDBCDriver driver;

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedJDBCService.class);

    @Inject
    public EmbeddedJDBCServiceImpl(AkServerInterface akServer,
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
    public Driver getDriver() {
        return driver;
    }

    @Override
    public Connection newConnection(Properties properties, Principal principal) throws SQLException {
        User user = null;
        if (principal != null) {
            if (principal instanceof User) {
                user = (User)principal;
            }
            else {
                // Translate from Java security realm (e.g., Jetty) to Akiban.
                user = reqs.securityService().getUser(principal.getName());
            }
        }
        if (user != null) {
            properties.put("user", user.getName());
            properties.put("database", user.getName());
        }
        else if (!properties.containsKey("user")) {
            properties.put("user", ""); // Avoid NPE.
        }
        Connection conn = driver.connect(JDBCDriver.URL, properties);
        if (user != null) {
            ((JDBCConnection)conn).getSession().put(SecurityService.SESSION_KEY, user);
        }
        return conn;
    }

    @Override
    public void start() {
        driver = new JDBCDriver(reqs);
        try {
            driver.register();
        }
        catch (SQLException ex) {
            throw new AkibanInternalException("Cannot register with JDBC", ex);
        }
    }

    @Override
    public void stop() {
        if (driver != null) {
            try {
                driver.deregister();
            }
            catch (SQLException ex) {
                logger.warn("Cannot deregister with JDBC", ex);
            }
            driver = null;
        }
    }

    @Override
    public void crash() {
        stop();
    }
}
