
package com.akiban.server.service.monitor;

import com.akiban.server.error.QueryLogCloseException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorServiceImpl implements Service, MonitorService, MonitorMXBean, JmxManageable 
{
    private static final String QUERY_LOG_PROPERTY = "akserver.querylog.enabled";
    private static final String QUERY_LOG_FILE_PROPERTY = "akserver.querylog.filename";
    private static final String QUERY_LOG_THRESHOLD = "akserver.querylog.exec_time_threshold";
    
    private static final Logger logger = LoggerFactory.getLogger(MonitorServiceImpl.class);

    private final ConfigurationService config;

    private Map<String,ServerMonitor> servers;

    private AtomicInteger sessionAllocator;
    private Map<Integer,SessionMonitor> sessions;

    private AtomicBoolean queryLogEnabled;
    private String queryLogFileName;
    private File queryLogFile;
    private BufferedWriter queryOut;

    private long execTimeThreshold;

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
        if (queryOut != null) {
            try {
                queryOut.close();
            } 
            catch (IOException ex) {
                throw new QueryLogCloseException(ex);
            }
        }
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
    public void registerSessionMonitor(SessionMonitor sessionMonitor) {
        SessionMonitor old = sessions.put(sessionMonitor.getSessionId(), sessionMonitor);
        assert ((old == null) || (old == sessionMonitor));
    }

    @Override
    public void deregisterSessionMonitor(SessionMonitor sessionMonitor) {
        SessionMonitor old = sessions.remove(sessionMonitor.getSessionId());
        assert ((old == null) || (old == sessionMonitor));
    }

    @Override
    public SessionMonitor getSessionMonitor(int sessionId) {
        return sessions.get(sessionId);
    }

    @Override
    public Collection<SessionMonitor> getSessionMonitors() {
        return sessions.values();
    }

    @Override
    public boolean isQueryLogEnabled() {
        return queryLogEnabled.get();
    }

    @Override
    public void logQuery(int sessionId, String sql, long duration, int rowsProcessed) {
        /*
         * If an execution time threshold has been specified but the query
         * to be logged is not larger than that execution time threshold
         * than we don't log anything.
         */
        if (execTimeThreshold > 0 && duration < execTimeThreshold) {
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
                queryOut.write(buffer.toString());
                queryOut.flush();
            }
        } 
        catch (IOException ex) {
            logger.warn("Failed to write to query log.", ex);
            /* disable query logging due to failure */
            queryLogEnabled.set(false);
        }
    }
    
    @Override
    public void logQuery(SessionMonitor sessionMonitor) {
        logQuery(sessionMonitor.getSessionId(), 
                 sessionMonitor.getCurrentStatement(),
                 sessionMonitor.getCurrentStatementDurationMillis(),
                 sessionMonitor.getRowsProcessed());
    }

    /* MonitorMXBean interface */

    @Override
    public void enableQueryLog() {
        if (! isQueryLogEnabled()) {
            queryLogEnabled.set(setUpQueryLog());
        }
    }

    @Override
    public void disableQueryLog() {
        queryLogEnabled.set(false);
        if (queryOut != null) {
            try {
                queryOut.close();
            } 
            catch (IOException ex) {
                logger.warn("Failed to close query log output stream.", ex);
                throw new QueryLogCloseException(ex);
            }
        }
    }

    @Override
    public void setQueryLogFileName(String fileName) {
        this.queryLogFileName = fileName;
        this.queryLogFile = null;        
    }

    @Override
    public String getQueryLogFileName() {
        return queryLogFileName;
    }
    
    @Override
    public synchronized void setExecutionTimeThreshold(long threshold) {
        execTimeThreshold = threshold;
    }
    
    @Override
    public synchronized long getExecutionTimeThreshold() {
        return execTimeThreshold;
    }
    
    /* JmxManageable interface */
    
    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Monitor", this, MonitorMXBean.class);
    }

    /**
     * create the necessary file on disk for the query log
     * if it does not already exist.
     * @return false on failure; true on success
     */
    private boolean setUpQueryLog() {
        if (queryLogFileName.isEmpty()) {
            logger.error("File name for query log was never set.");
            return false;
        }
        if (queryLogFile == null) {
            queryLogFile = new File(queryLogFileName);
        }
        try {
            if (queryLogFile.createNewFile()) {
                logger.debug("Query log file already existed. Appending to existing file.");
            }
        } 
        catch (IOException ex) {
            logger.error("Failed to create query log file", ex);
            return false;
        }
        FileWriter fstream;
        try {
            fstream = new FileWriter(queryLogFileName, true);
        } 
        catch (IOException ex) {
            logger.error("Failed to create FileWriter object for query log.", ex);
            return false;
        }
        queryOut = new BufferedWriter(fstream);
        logger.debug("Query log file ready for writing.");
        return true;
    }

}
