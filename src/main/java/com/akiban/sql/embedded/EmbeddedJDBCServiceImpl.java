/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.embedded;

import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.server.ServerServiceRequirements;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.instrumentation.InstrumentationService;
import com.akiban.server.service.routines.RoutineLoader;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;

import java.sql.Driver;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class EmbeddedJDBCServiceImpl implements EmbeddedJDBCService, Service {
    private final ServerServiceRequirements reqs;
    private JDBCDriver driver;

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedJDBCService.class);

    @Inject
    public EmbeddedJDBCServiceImpl(ConfigurationService config,
                                   DXLService dxlService,
                                   InstrumentationService instrumentation,
                                   SessionService sessionService,
                                   Store store,
                                   TreeService treeService,
                                   FunctionsRegistry functionsRegistry,
                                   IndexStatisticsService indexStatisticsService,
                                   T3RegistryService overloadResolutionService,
                                   RoutineLoader routineLoader,
                                   TransactionService txnService) {
        reqs = new ServerServiceRequirements(dxlService, instrumentation, 
                sessionService, store, treeService, functionsRegistry, 
                config, indexStatisticsService, overloadResolutionService, routineLoader, txnService);
    }

    @Override
    public Driver getDriver() {
        return driver;
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
