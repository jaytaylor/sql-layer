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

package com.foundationdb.sql.embedded;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.routines.ScriptEngineManagerProvider;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.security.User;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.sql.optimizer.rule.cost.CostModelFactory;
import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.sql.jdbc.ProxyDriverImpl;

import java.lang.reflect.*;
import java.security.Principal;
import java.sql.*;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class EmbeddedJDBCServiceImpl implements EmbeddedJDBCService, Service {
    private final ServerServiceRequirements reqs;
    private ScriptEngineManagerProvider scriptEngineManagerProvider;
    private JDBCDriver driver;
    private Driver proxyDriver;

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedJDBCService.class);

    @Inject
    public EmbeddedJDBCServiceImpl(LayerInfoInterface layerInfo,
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
                                   ScriptEngineManagerProvider scriptEngineManagerProvider) {
        reqs = new ServerServiceRequirements(layerInfo, dxlService, monitor,
                sessionService, store,
                config, indexStatisticsService, overloadResolutionService, 
                routineLoader, txnService, securityService, costModel,
                metricsService,
                serviceManager);
        this.scriptEngineManagerProvider = scriptEngineManagerProvider;
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
                // Translate from Java security realm (e.g., Jetty) to SQL Layer.
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
        Class<?> proxyDriverClazz;
        Constructor<?> proxyConstructor;
        driver = new JDBCDriver(reqs);
        try {
            proxyDriverClazz = Class.forName(ProxyDriverImpl.class.getName(), true, this.scriptEngineManagerProvider.getSafeClassLoader());
            proxyConstructor = proxyDriverClazz.getConstructor(Driver.class);
            Object proxyDriverInstanceObject = proxyConstructor.newInstance(driver);
            proxyDriver = (Driver) proxyDriverInstanceObject;
            driver.register();
            registerProxy(proxyDriver);
        }
        catch (ClassNotFoundException cnfe) {
            throw new AkibanInternalException("Cannot find proxy driver class", cnfe);
        }
        catch (NoSuchMethodException nsme) {
            throw new AkibanInternalException("Cannot find constructor method driver", nsme);
        }
        catch (IllegalAccessException iae) {
            throw new AkibanInternalException("Cannot access proxy driver constructor", iae);
        }
        catch (InstantiationException ie) {
            throw new AkibanInternalException("Cannot instantiate proxy driver", ie);
        }
        catch (InvocationTargetException ite){
            throw new AkibanInternalException("Cannot instantiate proxy driver", ite);
        }
        catch (SQLException ex) {
            throw new AkibanInternalException("Cannot register driver with JDBC", ex);
        }
    }

    @Override
    public void stop() {
        if (driver != null) {
            try {
                deregisterProxy(proxyDriver);
                driver.deregister();
            }
            catch (SQLException ex) {
                logger.warn("Cannot deregister embedded driver with JDBC", ex);
            }
            driver = null;
            proxyDriver = null;
        }
    }

    @Override
    public void crash() {
        stop();
    }

    private void registerProxy(Driver driver) throws SQLException {
        DriverManager.registerDriver(driver);
    }

    private void deregisterProxy(Driver driver) throws SQLException {
        try {
            Class<?> deregisterProxyDriverHelper = Class.forName("com.foundationdb.sql.jdbc.DeregisterProxyDriverHelper", true, proxyDriver.getClass().getClassLoader());
            Object dph = deregisterProxyDriverHelper.newInstance();
            Method method = deregisterProxyDriverHelper.getMethod("deregisterProxy", Driver.class);
            method.invoke(dph, proxyDriver);
        }
        catch (NoSuchMethodException nme) {
            throw new AkibanInternalException("deregisterPoxy method does not exist", nme);
        }
        catch (InvocationTargetException ite) {
            throw new AkibanInternalException("Cannot deregister proxy driver", ite);
        }
        catch (ClassNotFoundException cnfe) {
            throw new AkibanInternalException("Cannot find DeregisterProxyDriverHelper class", cnfe);
        }
        catch (IllegalAccessException iae) {
            throw new AkibanInternalException("Cannot access DeregisterProxyDriverHelper", iae);
        }
        catch (InstantiationException ie) {
            throw new AkibanInternalException("Cannot instantiate DeregisterProxyDriverHelper", ie);
        }
    }
}
