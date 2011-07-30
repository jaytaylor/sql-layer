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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.akiban.server.service.config.ConfigurationService;
import com.akiban.sql.pg.PostgresService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.sql.pg.PostgresServer;
import com.akiban.sql.pg.PostgresSessionTracer;

public class InstrumentationServiceImpl implements
    InstrumentationService, 
    Service<InstrumentationService>,
    InstrumentationMXBean,
    JmxManageable {
    
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
        if (queryLogFile == null) {
            queryLogFile = new File(queryLogFileName);
        }
        try {
            if (queryLogFile.createNewFile()) {
                LOGGER.info("Query log file already existed. Appending to existing file.");
            }
        } catch (IOException e) {
            LOGGER.error("Failed to create query log file", e);
            return false;
        }
        FileWriter fstream;
        try {
            fstream = new FileWriter(queryLogFileName, true);
        } catch (IOException e) {
            LOGGER.error("Failed to create FileWriter object for query log.", e);
            return false;
        }
        queryOut = new BufferedWriter(fstream);
        LOGGER.info("Query log file ready for writing.");
        return true;
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
        String enableLog = config.getProperty(QUERY_LOG_PROPERTY);
        this.queryLogEnabled = new AtomicBoolean(Boolean.parseBoolean(enableLog));
        queryLogFileName = config.getProperty(QUERY_LOG_FILE_PROPERTY);
        if (isQueryLogEnabled()) {
            setUpQueryLog();
        }
    }

    @Override
    public void stop() throws Exception {
        if (queryOut != null){
            queryOut.close();
        }
    }

    @Override
    public void crash() throws Exception {
        // anything to do?
    }

    @Override
    public boolean isQueryLogEnabled()
    {
        return queryLogEnabled.get();
    }
    
    @Override
    public void logQuery(int sessionId, String sql, long duration)
    {
        /*
         * format of each query log entry is:
         * sessionID    SQL text    Exec time in ns
         */
        StringBuilder buffer = new StringBuilder();
        buffer.append(sessionId);
        buffer.append('\t');
        buffer.append(sql);
        buffer.append('\t');
        buffer.append(duration);
        buffer.append('\n');
        try {
            synchronized(this) {
                queryOut.write(buffer.toString());
                queryOut.flush();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to write to query log.", e);
            /* disable query logging due to failure */
            queryLogEnabled.set(false);
        }
    }
    
    // InstrumentationMXBean interface

    @Override
    public void enableQueryLog()
    {
        if (! isQueryLogEnabled()) {
            queryLogEnabled.set(setUpQueryLog());
        }
    }

    @Override
    public void disableQueryLog()
    {
        queryLogEnabled.set(false);
        if (queryOut != null) {
            try {
                queryOut.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close query log output stream.", e);
            }
        }
    }

    @Override
    public void setQueryLogFileName(String fileName)
    {
        this.queryLogFileName = fileName;
        this.queryLogFile = null;        
    }

    @Override
    public String getQueryLogFileName()
    {
        return queryLogFileName;
    }
    
    // JmxManageable interface
    
    @Override
    public JmxObjectInfo getJmxObjectInfo()
    {
        return new JmxObjectInfo("Instrumentation", this, InstrumentationMXBean.class);
    }

    // InstrumentationServiceImpl interface

    @Inject
    public InstrumentationServiceImpl(ConfigurationService config) {
        this.config = config;
    }


    // state
    
    private static final String QUERY_LOG_PROPERTY = "akserver.querylog";
    private static final String QUERY_LOG_FILE_PROPERTY = "akserver.querylogfile";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(InstrumentationServiceImpl.class);
            
//    private final PostgresServer pgServer;
    private final ConfigurationService config;
    private AtomicBoolean queryLogEnabled;
    private String queryLogFileName;
    private File queryLogFile;
    private BufferedWriter queryOut;
    
}
