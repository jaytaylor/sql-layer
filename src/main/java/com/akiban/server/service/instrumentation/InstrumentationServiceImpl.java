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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.sql.pg.PostgresServer;
import com.akiban.sql.pg.PostgresSessionTracer;

public class InstrumentationServiceImpl implements
    InstrumentationService, Service<InstrumentationService> {
    
    public InstrumentationServiceImpl()
    {
        this.serviceManager = ServiceManagerImpl.get();
        String enableLog = serviceManager.getConfigurationService().getProperty(QUERY_LOG_PROPERTY, "");
        this.queryLogEnabled = new AtomicBoolean(Boolean.parseBoolean(enableLog));
        queryLogFileName = serviceManager.getConfigurationService().getProperty(QUERY_LOG_FILE_PROPERTY, "");
        if (isQueryLogEnabled()) {
            setUpQueryLog();
        }
    }
    
    /**
     * create the necessary file on disk for the query log
     * if it does not already exist.
     * @return false on failure; true on success
     */
    private boolean setUpQueryLog()
    {
        if (queryLogFileName.isEmpty()) {
            LOGGER.error("File name for query log was never set.");
            return false;
        }
        queryLogFile = new File(queryLogFileName);
        try {
            if (! queryLogFile.createNewFile()) {
                LOGGER.info("Query log file already exists. Appending to existing file.");
            }
        } catch (IOException e) {
            LOGGER.error("Failed to create query log file", e);
            return false;
        }
        return true;
    }
    
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

    @Override
    public boolean isQueryLogEnabled()
    {
        return queryLogEnabled.get();
    }

    @Override
    public void enableQueryLog()
    {
        queryLogEnabled.set(setUpQueryLog());
    }

    @Override
    public void disableQueryLog()
    {
        queryLogEnabled.set(false);
    }

    @Override
    public void setQueryLogFileName(String fileName)
    {
        this.queryLogFileName = fileName;
    }

    @Override
    public String getQueryLogFileName()
    {
        return queryLogFileName;
    }
    
    // state
    
    private static final String QUERY_LOG_PROPERTY = "akserver.querylog";
    private static final String QUERY_LOG_FILE_PROPERTY = "akserver.querylogfile";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(InstrumentationServiceImpl.class);
            
    private PostgresServer pgServer;
    private AtomicBoolean queryLogEnabled;
    private String queryLogFileName;
    private File queryLogFile;
    private ServiceManager serviceManager;
    
}
