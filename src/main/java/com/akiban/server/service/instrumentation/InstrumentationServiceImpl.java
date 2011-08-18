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
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicBoolean;

import com.akiban.server.error.QueryLogCloseException;
import com.akiban.server.service.config.ConfigurationService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;

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
    public void start() {
        String enableLog = config.getProperty(QUERY_LOG_PROPERTY);
        this.queryLogEnabled = new AtomicBoolean(Boolean.parseBoolean(enableLog));
        this.execTimeThreshold = Integer.parseInt(config.getProperty(QUERY_LOG_THRESHOLD));
        queryLogFileName = config.getProperty(QUERY_LOG_FILE_PROPERTY);
        if (isQueryLogEnabled()) {
            setUpQueryLog();
        }
    }

    @Override
    public void stop() {
        if (queryOut != null){
            try {
                queryOut.close();
            } catch (IOException e) {
                throw new QueryLogCloseException (e.getMessage());
            }
        }
    }

    @Override
    public void crash() {
        // anything to do?
    }

    // InstrumentationService interface
    
    @Override
    public boolean isQueryLogEnabled()
    {
        return queryLogEnabled.get();
    }

    
    @Override
    public void logQuery(int sessionId, String sql, long duration, int rowsProcessed)
    {
        /*
         * If an execution time threshold has been specified but the query
         * to be logged is not larger than that execution time threshold
         * than we don't log anything.
         */
        if (execTimeThreshold > 0 && duration < execTimeThreshold)
        {
            return;
        }
        /*
         * format of each query log entry is:
         * #
         * # timestamp
         * # session_id=sessionID
         * # execution_time=xxxx
         * SQL text
         * #
         * For example:
         * # 2011-08-18 15:08:11.071
         * # session_id=2
         * # execution_time=69824520
         * select * from tables;
         * #
         * # 2011-08-18 15:08:18.224
         * # session_id=2
         * # execution_time=3132589
         * select * from groups;
         * #
         * Execution time is output in nano-seconds
         */
        StringBuilder buffer = new StringBuilder();
        buffer.append("# ");
        buffer.append(new Timestamp(System.currentTimeMillis()));
        buffer.append("\n");
        buffer.append("# session_id=");
        buffer.append(sessionId);
        buffer.append("\n");
        buffer.append("# execution_time=");
        buffer.append(duration);
        buffer.append("\n");
        buffer.append(sql);
        buffer.append("\n#\n");
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
                throw new QueryLogCloseException (e.getMessage());
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
    
    @Override
    public synchronized void setExecutionTimeThreshold(long threshold)
    {
        execTimeThreshold = threshold;
    }
    
    @Override
    public synchronized long getExecutionTimeThreshold()
    {
        return execTimeThreshold;
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
    
    private static final String QUERY_LOG_PROPERTY = "akserver.querylog.enabled";
    private static final String QUERY_LOG_FILE_PROPERTY = "akserver.querylog.filename";
    private static final String QUERY_LOG_THRESHOLD = "akserver.querylog.exec_time_threshold";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(InstrumentationServiceImpl.class);
            
//    private final PostgresServer pgServer;
    private final ConfigurationService config;
    private AtomicBoolean queryLogEnabled;
    private String queryLogFileName;
    private File queryLogFile;
    private BufferedWriter queryOut;
    private long execTimeThreshold;
    
}
