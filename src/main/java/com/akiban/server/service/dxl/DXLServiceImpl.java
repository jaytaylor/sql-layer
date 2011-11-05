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

package com.akiban.server.service.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.ServiceNotStartedException;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DXLServiceImpl implements DXLService, Service<DXLService>, JmxManageable {

    private final Object MONITOR = new Object();

    private volatile DDLFunctions ddlFunctions;
    private volatile DMLFunctions dmlFunctions;
    private final SchemaManager schemaManager;
    private final Store store;
    private final TreeService treeService;
    private final SessionService sessionService;

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
        DDLFunctions localDdlFunctions = new HookableDDLFunctions(createDDLFunctions(middleman), hooks);
        DMLFunctions localDmlFunctions = new HookableDMLFunctions(createDMLFunctions(middleman, localDdlFunctions), hooks);
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
        return new BasicDDLFunctions(middleman, schemaManager, store, treeService);
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
        return Collections.<DXLFunctionsHook>singletonList(DXLReadWriteLockHook.only());
    }

    @Override
    public void crash() {
        BasicDXLMiddleman.destroy();
    }

    @Inject
    public DXLServiceImpl(SchemaManager schemaManager, Store store, TreeService treeService, SessionService sessionService) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.treeService = treeService;
        this.sessionService = sessionService;
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
