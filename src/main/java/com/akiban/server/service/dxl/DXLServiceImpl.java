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

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceNotStartedException;
import com.akiban.server.service.ServiceStartupException;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.List;

public class DXLServiceImpl implements DXLService, Service<DXLService>, JmxManageable {

    private final Object MONITOR = new Object();

    private volatile DDLFunctions ddlFunctions;
    private volatile DMLFunctions dmlFunctions;
    private final SchemaManager schemaManager;
    private final Store store;
    private final TreeService treeService;

    private final DXLMXBean bean = new DXLMXBeanImpl(this);

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("DXL", bean, DXLMXBean.class);
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
    public void start() throws Exception {
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
    }

    DMLFunctions createDMLFunctions(BasicDXLMiddleman middleman, DDLFunctions newlyCreatedDDLF) {
        return new BasicDMLFunctions(middleman, schemaManager, store, treeService, newlyCreatedDDLF);
    }

    DDLFunctions createDDLFunctions(BasicDXLMiddleman middleman) {
        return new BasicDDLFunctions(middleman, schemaManager, store, treeService);
    }

    @Override
    public void stop() throws Exception {
        synchronized (MONITOR) {
            if (ddlFunctions == null) {
                throw new ServiceNotStartedException();
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
            throw new ServiceNotStartedException();
        }
        return ret;
    }

    @Override
    public DMLFunctions dmlFunctions() {
        final DMLFunctions ret = dmlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException();
        }
        return ret;
    }

    protected List<DXLFunctionsHook> getHooks() {
        return Collections.<DXLFunctionsHook>singletonList(DXLReadWriteLockHook.only());
    }
    
    @Override
    public void crash() throws Exception {
        BasicDXLMiddleman.destroy();
    }

    @Inject
    public DXLServiceImpl(SchemaManager schemaManager, Store store, TreeService treeService) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.treeService = treeService;
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
}
