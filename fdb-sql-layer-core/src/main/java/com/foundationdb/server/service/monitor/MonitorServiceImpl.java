/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.monitor;

import com.foundationdb.server.error.QueryLogCloseException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorServiceImpl implements Service, MonitorService
{
    private static final String QUERY_LOG_PROPERTY = "fdbsql.querylog.enabled";
    private static final String QUERY_LOG_FILE_PROPERTY = "fdbsql.querylog.filename";
    private static final String QUERY_LOG_THRESHOLD = "fdbsql.querylog.exec_threshold_ms";
    
    private static final Logger logger = LoggerFactory.getLogger(MonitorServiceImpl.class);

    public static final Session.Key<SessionMonitor> SESSION_KEY = 
        Session.Key.named("SESSION_MONITOR");

    private final ConfigurationService config;

    private Map<String,ServerMonitor> servers;

    private AtomicInteger sessionAllocator;
    private Map<Integer,SessionMonitor> sessions;

    private volatile String queryLogFileName;
    private volatile boolean isQueryLogEnabled;
    private volatile long queryLogThresholdMillis;
    private BufferedWriter queryLogWriter;
    
    private Map<String, UserMonitor> users;

    @Inject
    public MonitorServiceImpl(ConfigurationService config) {
        this.config = config;
    }

    /* Service interface */

    @Override
    public void start() {
        servers = new ConcurrentHashMap<>();

        sessionAllocator = new AtomicInteger();
        sessions = new ConcurrentHashMap<>();
        users = new ConcurrentHashMap<>();

        this.isQueryLogEnabled = false;
        this.queryLogThresholdMillis = Integer.parseInt(config.getProperty(QUERY_LOG_THRESHOLD));
        this.queryLogFileName = config.getProperty(QUERY_LOG_FILE_PROPERTY);
        setQueryLogEnabled(Boolean.parseBoolean(config.getProperty(QUERY_LOG_PROPERTY)));
    }

    @Override
    public void stop() {
        setQueryLogEnabled(false);
    }

    @Override
    public void crash() {
        stop();
    }

    /* MonitorService interface */
    
    @Override
    public void registerServerMonitor(ServerMonitor serverMonitor) {
        ServerMonitor old = servers.put(serverMonitor.getServerType(), serverMonitor);
        assert ((old == null) || (old == serverMonitor));
    }

    @Override
    public void deregisterServerMonitor(ServerMonitor serverMonitor) {
        ServerMonitor old = servers.remove(serverMonitor.getServerType());
        assert ((old == null) || (old == serverMonitor));
    }

    @Override
    public Map<String,ServerMonitor> getServerMonitors() {
        return servers;
    }

    @Override
    public int allocateSessionId() {
        return sessionAllocator.incrementAndGet();
    }

    @Override
    public void registerSessionMonitor(SessionMonitor sessionMonitor, Session session) {
        SessionMonitor old = sessions.put(sessionMonitor.getSessionId(), sessionMonitor);
        assert ((old == null) || (old == sessionMonitor));
        session.put(SESSION_KEY, sessionMonitor);
    }

    @Override
    public void deregisterSessionMonitor(SessionMonitor sessionMonitor, Session session) {
        SessionMonitor old = sessions.remove(sessionMonitor.getSessionId());
        assert ((old == null) || (old == sessionMonitor));
        session.remove(SESSION_KEY);
    }

    @Override
    public SessionMonitor getSessionMonitor(int sessionId) {
        return sessions.get(sessionId);
    }

    @Override
    public SessionMonitor getSessionMonitor(Session session) {
        return session.get(SESSION_KEY);
    }

    @Override
    public Collection<SessionMonitor> getSessionMonitors() {
        return sessions.values();
    }

    @Override
    public void logQuery(int sessionId, String sql, long duration, int rowsProcessed) {
        /*
         * If an execution time threshold has been specified but the query
         * to be logged is not larger than that execution time threshold
         * than we don't log anything.
         */
        if (queryLogThresholdMillis > 0 && duration < queryLogThresholdMillis) {
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
         * Execution time is output in milliseconds
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
                queryLogWriter.write(buffer.toString());
                queryLogWriter.flush();
            }
        } 
        catch (IOException ex) {
            logger.warn("Failed to write to query log.", ex);
            /* disable query logging due to failure */
            isQueryLogEnabled = false;
        }
    }
    
    @Override
    public void logQuery(SessionMonitor sessionMonitor) {
        logQuery(sessionMonitor.getSessionId(), 
                 sessionMonitor.getCurrentStatement(),
                 sessionMonitor.getCurrentStatementDurationMillis(),
                 sessionMonitor.getRowsProcessed());
    }

    /** Register the given User monitor. */
    @Override
    public void registerUserMonitor (UserMonitor userMonitor) {
        UserMonitor monitor = users.put(userMonitor.getUserName(), userMonitor);
        assert (monitor == null || monitor == userMonitor);
    }

    /** Deregister the monitor for the given user */
    @Override
    public void deregisterUserMonitor (String userName) {
        users.remove(userName);
    }
    
    /** Deregister the given user monitor. */
    @Override
    public void deregisterUserMonitor (UserMonitor userMonitor) {
        UserMonitor monitor = users.remove(userMonitor.getUserName());
        assert (monitor== null || monitor == userMonitor);
    }
    
    /** Get the user monitor for the given user name. */
    @Override 
    public UserMonitor getUserMonitor(String userName) {
        return users.get(userName); 
    }
    
    /** Get the user monitor for the session user */
    @Override
    public UserMonitor getUserMonitor(Session session) {
       return session.get(SESSION_KEY).getUserMonitor();
    }

    /** Get all the user monitors. */
    @Override
    public Collection<UserMonitor> getUserMonitors() {
        return users.values();
    }

    /* Query Log Control */

    @Override
    public boolean isQueryLogEnabled() {
        return isQueryLogEnabled;
    }

    @Override
    public synchronized void setQueryLogEnabled(boolean enabled) {
        if(isQueryLogEnabled == enabled) {
            return;
        }
        if(enabled) {
            isQueryLogEnabled = setUpQueryLog();
        } else {
            isQueryLogEnabled = false;
            try {
                queryLogWriter.close();
            } catch(IOException ex) {
                logger.warn("Failed to close query log output stream.", ex);
                throw new QueryLogCloseException(ex);
            }
            queryLogWriter = null;
        }
    }

    @Override
    public void setQueryLogFileName(String fileName) {
        this.queryLogFileName = fileName;
    }

    @Override
    public String getQueryLogFileName() {
        return queryLogFileName;
    }

    @Override
    public void setQueryLogThresholdMillis(long threshold) {
        queryLogThresholdMillis = threshold;
    }

    @Override
    public long getQueryLogThresholdMillis() {
        return queryLogThresholdMillis;
    }

    /* Internal */

    /**
     * Open, and create if necessary, the query log file.
     * @return false on failure; true on success
     */
    private boolean setUpQueryLog() {
        assert queryLogWriter == null;

        if (queryLogFileName.isEmpty()) {
            logger.error("File name for query log was never set.");
            return false;
        }
        File queryLogFile = new File(queryLogFileName);
        try {
            if(queryLogFile.createNewFile()) {
                logger.debug("Query log file already existed. Appending to existing file.");
            }
        } catch(IOException ex) {
            logger.error("Failed to create query log file", ex);
            return false;
        }
        FileWriter writer;
        try {
            writer = new FileWriter(queryLogFileName, true);
        } catch(IOException ex) {
            logger.error("Failed to create FileWriter object for query log.", ex);
            return false;
        }
        this.queryLogWriter = new BufferedWriter(writer);
        logger.debug("Query log file ready for writing.");
        return true;
    }
}
