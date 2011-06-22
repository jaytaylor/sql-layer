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

package com.akiban.server.service.instrumentation;

import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.sql.pg.PostgresServer;
import com.akiban.sql.pg.PostgresSessionTracer;

public class InstrumentationServiceImpl implements
    InstrumentationService, Service<InstrumentationService> {
    
    // InstrumentationService interface
    
    public synchronized PostgresSessionTracer createSqlSessionTracer(int sessionId) {
        PostgresSessionTracer ret = new PostgresSessionTracer(sessionId, pgServer.isInstrumentationEnabled());
        return ret;
    }
    
    public synchronized PostgresSessionTracer getSqlSessionTracer(int sessionId) {
        return (PostgresSessionTracer)pgServer.getConnection(sessionId).getSessionTracer();
    }

    // Service interface
    
    @Override
    public InstrumentationService cast() {
        return this;
    }

    @Override
    public Class<InstrumentationService> castClass() {
        return InstrumentationService.class;
    }

    @Override
    public void start() throws Exception {
        pgServer = ServiceManagerImpl.get().getPostgresService().getServer();
    }

    @Override
    public void stop() throws Exception {
        // anything to do?
    }

    @Override
    public void crash() throws Exception {
        // anything to do?
    }

    @Override
    public boolean isEnabled() {
        return pgServer.isInstrumentationEnabled();
    }

    @Override
    public void enable() {
        pgServer.enableInstrumentation();
    }

    @Override
    public void disable() {
        pgServer.disableInstrumentation();
    }

    @Override
    public boolean isEnabled(int sessionId) {
        return pgServer.isInstrumentationEnabled(sessionId);
    }

    @Override
    public void enable(int sessionId) {
        pgServer.enableInstrumentation(sessionId);
    }

    @Override
    public void disable(int sessionId) {  
        pgServer.disableInstrumentation(sessionId);
    }
  
    // state
            
    private PostgresServer pgServer;

}
