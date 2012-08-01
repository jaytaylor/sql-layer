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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.ServiceNotStartedException;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.t3expressions.T3RegistryService;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DXLServiceImpl implements DXLService, Service<DXLService>, JmxManageable {

    private final Object MONITOR = new Object();

    private volatile HookableDDLFunctions ddlFunctions;
    private volatile DMLFunctions dmlFunctions;
    private final SchemaManager schemaManager;
    private final Store store;
    private final TreeService treeService;
    private final SessionService sessionService;
    private final IndexStatisticsService indexStatisticsService;
    private final ConfigurationService configService;
    private final T3RegistryService t3Registry;

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("DXL", new DXLMXBeanImpl(this, store(), sessionService), DXLMXBean.class);
    }

    @Override
    public DXLService cast() {
        return this;
    }

    @Override
    public Class<DXLService> castClass() {
        return DXLService.class;
    }

    @Override
    public void start() {
        List<DXLFunctionsHook> hooks = getHooks();
        BasicDXLMiddleman middleman = BasicDXLMiddleman.create();
        HookableDDLFunctions localDdlFunctions
                = new HookableDDLFunctions(createDDLFunctions(middleman), hooks,sessionService);
        DMLFunctions localDmlFunctions = new HookableDMLFunctions(createDMLFunctions(middleman, localDdlFunctions),
                hooks, sessionService);
        synchronized (MONITOR) {
            if (ddlFunctions != null) {
                throw new ServiceStartupException("service already started");
            }
            ddlFunctions = localDdlFunctions;
            dmlFunctions = localDmlFunctions;
        }
        recreateGroupIndexes(startupGiRecreatePredicate());
    }

    DMLFunctions createDMLFunctions(BasicDXLMiddleman middleman, DDLFunctions newlyCreatedDDLF) {
        return new BasicDMLFunctions(middleman, schemaManager, store, treeService, newlyCreatedDDLF);
    }

    DDLFunctions createDDLFunctions(BasicDXLMiddleman middleman) {
        return new BasicDDLFunctions(middleman, schemaManager, store, treeService, indexStatisticsService, configService, t3Registry);
    }

    @Override
    public void stop() {
        synchronized (MONITOR) {
            if (ddlFunctions == null) {
                throw new ServiceNotStartedException("DDL Functions stop");
            }
            ddlFunctions = null;
            dmlFunctions = null;
            BasicDXLMiddleman.destroy();
        }
    }

    @Override
    public DDLFunctions ddlFunctions() {
        final DDLFunctions ret = ddlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DDL Functions");
        }
        return ret;
    }

    @Override
    public DMLFunctions dmlFunctions() {
        final DMLFunctions ret = dmlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DML Functions");
        }
        return ret;
    }

    @Override
    public void recreateGroupIndexes(GroupIndexRecreatePredicate predicate) {
        Session session = sessionService.createSession();
        try {
            DDLFunctions ddl = ddlFunctions();
            AkibanInformationSchema ais = ddl.getAIS(session);
            Map<String,List<GroupIndex>> gisByGroup = new HashMap<String, List<GroupIndex>>();
            for (com.akiban.ais.model.Group group : ais.getGroups().values()) {
                ArrayList<GroupIndex> groupGis = new ArrayList<GroupIndex>(group.getIndexes());
                for (Iterator<GroupIndex> iterator = groupGis.iterator(); iterator.hasNext(); ) {
                    GroupIndex gi = iterator.next();
                    boolean shouldRecreate = predicate.shouldRecreate(gi);
                    groupIndexMayNeedRecreating(gi, shouldRecreate);
                    if (!shouldRecreate) {
                        iterator.remove();
                    }
                }
                gisByGroup.put(group.getName(), groupGis);
            }
            for (Map.Entry<String,List<GroupIndex>> entry : gisByGroup.entrySet()) {
                List<GroupIndex> gis = entry.getValue();
                List<String> giNames = new ArrayList<String>(gis.size());
                for (Index gi : gis) {
                    giNames.add(gi.getIndexName().getName());
                }
                ddl.dropGroupIndexes(session, entry.getKey(), giNames);
                ddl.createIndexes(session, gis);
            }
        } finally {
            session.close();
        }
    }

    protected List<DXLFunctionsHook> getHooks() {
        List<DXLFunctionsHook> hooks = new ArrayList<DXLFunctionsHook>();
        hooks.add(DXLReadWriteLockHook.only());
        hooks.add(new DXLTransactionHook(treeService));
        return hooks;
    }

    @Override
    public void crash() {
        BasicDXLMiddleman.destroy();
    }

    @Inject
    public DXLServiceImpl(SchemaManager schemaManager, Store store, TreeService treeService, SessionService sessionService,
                          IndexStatisticsService indexStatisticsService, ConfigurationService configService, T3RegistryService t3Registry) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.treeService = treeService;
        this.sessionService = sessionService;
        this.indexStatisticsService = indexStatisticsService;
        this.configService = configService;
        this.t3Registry = t3Registry;
    }

    // for use by subclasses

    protected final SchemaManager schemaManager() {
        return schemaManager;
    }

    protected final Store store() {
        return store;
    }

    protected final TreeService treeService() {
        return treeService;
    }

    protected final IndexStatisticsService indexStatisticsService() {
        return indexStatisticsService;
    }

    protected final ConfigurationService configService() {
        return configService;
    }

    protected final T3RegistryService t3Registry() {
        return t3Registry;
    }

    protected final Session session() {
        return null;
    }

    /**
     * Invoked when a group index may need recreating due to an invocation of {@linkplain #recreateGroupIndexes}. This
     * method will be invoked <em>before</em> the index has been rebuilt; it'll still be active.
     * @param groupIndex the index that's about to be recreated
     * @param needsRecreating whether the index will be recreated
     */
    protected void groupIndexMayNeedRecreating(GroupIndex groupIndex, boolean needsRecreating) {
        // nothing
    }

    private GroupIndexRecreatePredicate startupGiRecreatePredicate() {
        return new GroupIndexRecreatePredicate() {
            @Override
            public boolean shouldRecreate(GroupIndex index) {
                return ! index.isValid();
            }
        };
    }
}
