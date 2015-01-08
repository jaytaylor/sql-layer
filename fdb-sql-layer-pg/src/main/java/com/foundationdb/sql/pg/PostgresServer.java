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

package com.foundationdb.sql.pg;

import com.foundationdb.sql.server.CacheCounters;
import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.sql.server.ServerStatementCache;

import com.foundationdb.server.error.InvalidPortException;
import com.foundationdb.server.service.metrics.LongMetric;
import com.foundationdb.server.service.monitor.MonitorStage;
import com.foundationdb.server.service.monitor.ServerMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/** The PostgreSQL server.
 * Listens of a given port and spawns <code>PostgresServerConnection</code> threads
 * to process requests.
 * Also keeps global state for shutdown and inter-connection communication like cancel.
*/
public class PostgresServer implements Runnable, PostgresMXBean, ServerMonitor {
    public static final String COMMON_PROPERTIES_PREFIX = "fdbsql.sql.";
    public static final String SERVER_PROPERTIES_PREFIX = "fdbsql.postgres.";
    protected static final String SERVER_TYPE = "Postgres";
    private static final String THREAD_NAME_PREFIX = "PostgresServer_Accept-"; // Port is appended
    private static final String BYTES_IN_METRIC_NAME = "PostgresBytesIn";
    private static final String BYTES_OUT_METRIC_NAME = "PostgresBytesOut";

    protected static enum AuthenticationType {
        NONE, CLEAR_TEXT, MD5, GSS
    };

    private final Properties properties;
    private final int port;
    private final String host;
    private final ServerServiceRequirements reqs;
    private ServerSocket socket = null;
    private volatile boolean running = false;
    private volatile long startTimeMillis, startTimeNanos;
    private boolean listening = false;
    private int nconnections = 0;
    private Map<Integer,PostgresServerConnection> connections =
        new HashMap<>();
    private Thread thread;
    // AIS-dependent state
    private volatile int statementCacheCapacity;
    private final Map<ObjectLongPair,ServerStatementCache<PostgresStatement>> statementCaches =
        new HashMap<>(); // key and aisGeneration
    // end AIS-dependent state
    private volatile Date overrideCurrentTime;
    private final CacheCounters cacheCounters = new CacheCounters();
    private AuthenticationType authenticationType;
    private Subject gssLogin;
    private final int slowLimit;
    private final int hardLimit;

    private static final Logger logger = LoggerFactory.getLogger(PostgresServer.class);

    public PostgresServer(ServerServiceRequirements reqs) {
        this.reqs = reqs;
        properties = reqs.config().deriveProperties(COMMON_PROPERTIES_PREFIX);
        properties.putAll(reqs.config().deriveProperties(SERVER_PROPERTIES_PREFIX));

        String portString = properties.getProperty("port");
        port = Integer.parseInt(portString);
        if (port <= 0)
            throw new InvalidPortException(port);
        host = properties.getProperty("host");
        
        String capacityString = properties.getProperty("statementCacheCapacity");
        statementCacheCapacity = Integer.parseInt(capacityString);
        
        slowLimit = Integer.parseInt(properties.getProperty("connection_slow_limit", "250"));
        hardLimit = Integer.parseInt(properties.getProperty("connection_hard_limit", "500"));
    }

    public Properties getProperties() {
        return properties;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    /** Called from the (Main's) main thread to start a server
        running in its own thread. */
    public void start() {
        running = true;
        startTimeMillis = System.currentTimeMillis();
        startTimeNanos = System.nanoTime();
        thread = new Thread(this, THREAD_NAME_PREFIX + getPort());
        thread.start();
    }

    /** Called from the main thread to shutdown a server. */
    public void stop() {
        ServerSocket socket;
        synchronized(this) {
            // Service might shutdown before we've even got server socket created.
            running = listening = false;
            socket = this.socket;
        }
        if (socket != null) {
            // Can only wake up by closing socket inside whose accept() we are blocked.
            try {
                socket.close();
            }
            catch (IOException ex) {
            }
        }

        Collection<PostgresServerConnection> conns;
        synchronized (this) {
            // Get a copy so they can remove themselves from stop().
            conns = new ArrayList<>(connections.values());
        }
        for (PostgresServerConnection connection : conns) {
            connection.stop();
        }

        if (thread != null) {
            try {
                // Wait a bit, but don't hang up shutdown if thread is wedged.
                thread.join(500);
                if (thread.isAlive())
                    logger.warn("Server still running.");
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    @Override
    public void run() {
        computeAuthenticationType();
        logger.info("Starting Postgres server listening on {}:{} with authentication {}", host, port, authenticationType);
        Random rand = new Random();
        LongMetric bytesInMetric = null, bytesOutMetric = null;
        try {
            bytesInMetric = reqs.metricsService().addLongMetric(BYTES_IN_METRIC_NAME);
            bytesOutMetric = reqs.metricsService().addLongMetric(BYTES_OUT_METRIC_NAME);
            reqs.monitor().registerServerMonitor(this);
            synchronized(this) {
                if (!running) return;
                // 50 here was taken from the shorter new ServerSocket(port)
                socket = new ServerSocket(port, 50, InetAddress.getByName(host));
                listening = true;
            }
            while (running) {
                Socket sock = socket.accept();
                
                // If we're running too many connections, slow down...
                if (connections.size() > hardLimit) {
                    logger.warn("Connection hard limit exceeded, wait for connections to close...");
                    do {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ex) {
                        }
                    } while (connections.size() > hardLimit);
                } else if (connections.size() > slowLimit) {
                    logger.warn("Connection slowdown limit exceeded, delaying connection start...");
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ex) {
                    }
                }
                
                int sessionId = reqs.monitor().allocateSessionId();
                int secret = rand.nextInt();
                PostgresServerConnection connection = 
                    new PostgresServerConnection(this, 
                                                 sock, sessionId, secret, 
                                                 bytesInMetric, bytesOutMetric,
                                                 reqs);
                nconnections++;
                connections.put(sessionId, connection);
                connection.start();
            }
        }
        catch (Exception ex) {
            if (running)
                logger.warn("Error in server", ex);
        }
        finally {
            if (socket != null) {
                try {
                    socket.close();
                }
                catch (IOException ex) {
                }
            }
            reqs.monitor().deregisterServerMonitor(this);
            reqs.metricsService().removeMetric(bytesOutMetric);
            reqs.metricsService().removeMetric(bytesInMetric);
            running = false;
        }
    }

    public synchronized boolean isListening() {
        return listening;
    }

    public synchronized PostgresServerConnection getConnection(int sessionId) {
        return connections.get(sessionId);
    }
   
    public synchronized void removeConnection(int sessionId) {
        connections.remove(sessionId);
    }
    
    public synchronized Collection<PostgresServerConnection> getConnections() {
        return new ArrayList<>(connections.values());
    }

    @Override
    public String getSqlString(int sessionId) {
        return getConnection(sessionId).getSessionMonitor().getCurrentStatement();
    }
    
    @Override
    public String getRemoteAddress(int sessionId) {
        return getConnection(sessionId).getSessionMonitor().getRemoteAddress();
    }

    @Override
    public void cancelQuery(int sessionId) {
        getConnection(sessionId).cancelQuery(null, "JMX");
    }

    @Override
    public void killConnection(int sessionId) {
        PostgresServerConnection conn = getConnection(sessionId);
        conn.cancelQuery("your session being disconnected", "JMX");
        conn.waitAndStop();
    }

    void cleanStatementCaches(long newGeneration) {
        Set<Long> activeGenerations = reqs.dxl().ddlFunctions().getActiveGenerations();
        logger.debug("Cleaning statement caches except {} (now {})", 
                     activeGenerations, newGeneration);
        synchronized (statementCaches) {
            Iterator<Map.Entry<ObjectLongPair,ServerStatementCache<PostgresStatement>>> it = statementCaches.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<ObjectLongPair,ServerStatementCache<PostgresStatement>> entry = it.next();
                if (!activeGenerations.contains(entry.getKey().longVal)) {
                    entry.getValue().invalidate(); // It may be a while before a connection gets a new one.
                    it.remove();
                }
            }
        }
    }

    /** This is the version for use by connections. */
    public ServerStatementCache<PostgresStatement> getStatementCache(Object key, long aisGeneration) {
        if (statementCacheCapacity <= 0)
            return null;

        ObjectLongPair fullKey = new ObjectLongPair(key, aisGeneration);
        ServerStatementCache<PostgresStatement> statementCache;
        synchronized (statementCaches) {
            statementCache = statementCaches.get(fullKey);
            if (statementCache == null) {
                // No cache => recent DDL, reasonable time to do a little cleaning
                cleanStatementCaches(aisGeneration);
                statementCache = new ServerStatementCache<>(cacheCounters, statementCacheCapacity);
                statementCaches.put(fullKey, statementCache);
            }
        }
        return statementCache;
    }

    @Override
    public int getStatementCacheCapacity() {
        return statementCacheCapacity;
    }

    @Override
    public void setStatementCacheCapacity(int capacity) {
        statementCacheCapacity = capacity;
        synchronized (statementCaches) {
            for (ServerStatementCache<PostgresStatement> statementCache : statementCaches.values()) {
                statementCache.setCapacity(capacity);
            }
            if (capacity <= 0) {
                statementCaches.clear();
            }
        }
    }

    @Override
    public int getStatementCacheHits() {
        return cacheCounters.getHits();
    }

    @Override
    public int getStatementCacheMisses() {
        return cacheCounters.getMisses();
    }
    
    @Override
    public void resetStatementCache() {
        synchronized (statementCaches) {
            cacheCounters.reset();
            for (ServerStatementCache<PostgresStatement> statementCache : statementCaches.values()) {
                statementCache.reset();
            }        
        }
    }

    @Override
    public Set<Integer> getCurrentSessions() {
        return new HashSet<>(connections.keySet());

    }

    @Override
    public Date getStartTime(int sessionId) {
        return new Date(getConnection(sessionId).getSessionMonitor().getStartTimeMillis());
    }

    @Override
    public long getProcessingTime(int sessionId) {
        return getConnection(sessionId).getSessionMonitor().getNonIdleTimeNanos();
    }

    @Override
    public long getEventTime(int sessionId, String eventName) {
        return getConnection(sessionId).getSessionMonitor().getLastTimeStageNanos(MonitorStage.valueOf(eventName));
    }

    @Override
    public long getTotalEventTime(int sessionId, String eventName) {
        return getConnection(sessionId).getSessionMonitor().getTotalTimeStageNanos(MonitorStage.valueOf(eventName));
    }

    @Override
    public long getUptime()
    {
        return (System.nanoTime() - startTimeNanos);
    }

    /** For testing, set the server's idea of the current time. */
    public void setOverrideCurrentTime(Date overrideCurrentTime) {
        this.overrideCurrentTime = overrideCurrentTime;
    }

    public Date getOverrideCurrentTime() {
        return overrideCurrentTime;
    }

    public AuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    protected void computeAuthenticationType() {
        if (properties.getProperty("gssConfigName") != null) {
            authenticationType = AuthenticationType.GSS;
        }
        else {
            String login = properties.getProperty("login", "none");
            if (login.equals("none")) {
                authenticationType = AuthenticationType.NONE;
            }
            else if (login.equals("password")) {
                authenticationType = AuthenticationType.CLEAR_TEXT;
            }
            else if (login.equals("md5")) {
                authenticationType = AuthenticationType.MD5;
            }
            else {
                throw new IllegalArgumentException("Invalid login property: " +
                                                   login);
            }
        }
    }

    /** Login to the KDC as the service for this server (using a keytab)
     * and return the <code>Subject</code> that can then be used to
     * authenticate a client.
     */
    public synchronized Subject getGSSLogin() throws LoginException {
        if (gssLogin == null) {
            LoginContext lc = new LoginContext(properties.getProperty("gssConfigName"));
            lc.login();
            gssLogin = lc.getSubject();
        }
        return gssLogin;
    }

    /* ServerMonitor */

    @Override
    public String getServerType() {
        return SERVER_TYPE;
    }

    @Override
    public int getLocalPort() {
        if (listening)
            return port;
        else
            return -1;
    }

    @Override
    public String getLocalHost() {
        if (listening)
            return host;
        else
            return null;
    }

    @Override
    public long getStartTimeMillis() {
        return startTimeMillis;
    }
    
    @Override
    public int getSessionCount() {
        return nconnections;
    }

}
