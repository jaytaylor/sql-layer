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

package com.akiban.server.service.d_l;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceNotStartedException;
import com.akiban.server.service.ServiceStartupException;
import com.akiban.server.service.jmx.JmxManageable;

public class DStarLServiceImpl implements DStarLService, Service<DStarLService>, JmxManageable {

    private final Object MONITOR = new Object();

    private volatile DDLFunctions ddlFunctions;
    private volatile DMLFunctions dmlFunctions;

    private final DStarLMXBean bean = new MyDStarLMXBeanImpl(this);

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("DStarL", bean, DStarLMXBean.class);
    }

    @Override
    public DStarLService cast() {
        return this;
    }

    @Override
    public Class<DStarLService> castClass() {
        return DStarLService.class;
    }

    @Override
    public void start() throws Exception {
        synchronized (MONITOR) {
            if (ddlFunctions != null) {
                throw new ServiceStartupException("service already started");
            }
            ddlFunctions = new DDLFunctionsImpl();
            dmlFunctions = new DMLFunctionsImpl(ddlFunctions);
        }
    }

    @Override
    public void stop() throws Exception {
        synchronized (MONITOR) {
            if (ddlFunctions == null) {
                throw new ServiceNotStartedException();
            }
            ddlFunctions = null;
            dmlFunctions = null;
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
    
    @Override
    public void crash() throws Exception {
    }
    
    
}
