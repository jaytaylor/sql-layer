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

package com.akiban.sql.server;

import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.instrumentation.InstrumentationService;
import com.akiban.server.service.routines.RoutineLoader;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.t3expressions.T3RegistryService;

public final class ServerServiceRequirements {

    public ServerServiceRequirements(DXLService dxlService,
                                     InstrumentationService instrumentation,
                                     SessionService sessionService,
                                     Store store,
                                     TreeService treeService,
                                     FunctionsRegistry functionsRegistry,
                                     ConfigurationService config,
                                     IndexStatisticsService indexStatistics,
                                     T3RegistryService t3RegistryService,
                                     RoutineLoader routineLoader) {
        this.instrumentation = instrumentation;
        this.dxlService = dxlService;
        this.sessionService = sessionService;
        this.store = store;
        this.treeService = treeService;
        this.functionsRegistry = functionsRegistry;
        this.config = config;
        this.indexStatistics = indexStatistics;
        this.t3RegistryService = t3RegistryService;
        this.routineLoader = routineLoader;
    }

    public InstrumentationService instrumentation() {
        return instrumentation;
    }

    public DXLService dxl() {
        return dxlService;
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

    private final InstrumentationService instrumentation;
    private final DXLService dxlService;
    private final SessionService sessionService;
    private final Store store;
    private final TreeService treeService;
    private final FunctionsRegistry functionsRegistry;
    private final ConfigurationService config;
    private final IndexStatisticsService indexStatistics;
    private final T3RegistryService t3RegistryService;
    private final RoutineLoader routineLoader;
}
