/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.pg;

import com.akiban.sql.server.CacheCounters;
import com.akiban.sql.server.ServerServiceRequirements;
import com.akiban.sql.server.ServerStatementCache;

import com.akiban.server.error.InvalidPortException;
import com.akiban.server.service.monitor.MonitorStage;
import com.akiban.server.service.monitor.ServerMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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

/** The PostgreSQL server.
 * Listens of a given port and spawns <code>PostgresServerConnection</code> threads
 * to process requests.
 * Also keeps global state for shutdown and inter-connection communication like cancel.
*/
public class PostgresServer implements Runnable, PostgresMXBean, ServerMonitor {
    public static final String SERVER_PROPERTIES_PREFIX = "akserver.postgres.";
    protected static final String SERVER_TYPE = "Postgres";

    private final Properties properties;
    private final int port;
    private final ServerServiceRequirements reqs;
    private ServerSocket socket = null;
    private volatile boolean running = false;
    private volatile long startTimeMillis, startTimeNanos;
    private boolean listening = false;
    private int nconnections = 0;
    private Map<Integer,PostgresServerConnection> connections =
        new HashMap<Integer,PostgresServerConnection>();
    private Thread thread;
    // AIS-dependent state
    private volatile int statementCacheCapacity;
    private final Map<ObjectLongPair,ServerStatementCache<PostgresStatement>> statementCaches =
        new HashMap<ObjectLongPair,ServerStatementCache<PostgresStatement>>(); // key and aisGeneration
    // end AIS-dependent state
    private volatile Date overrideCurrentTime;
    private final CacheCounters cacheCounters = new CacheCounters();

    private static final Logger logger = LoggerFactory.getLogger(PostgresServer.class);

    public PostgresServer(ServerServiceRequirements reqs) {
        this.reqs = reqs;
        properties = reqs.config().deriveProperties(SERVER_PROPERTIES_PREFIX);

        String portString = properties.getProperty("port");
        port = Integer.parseInt(portString);
        if (port <= 0)
            throw new InvalidPortException(port);
        
        String capacityString = properties.getProperty("statementCacheCapacity");
        statementCacheCapacity = Integer.parseInt(capacityString);
    }

    public Properties getProperties() {
        return properties;
    }

    public int getPort() {
        return port;
    }

    /** Called from the (AkServer's) main thread to start a server
        running in its own thread. */
    public void start() {
        running = true;
        startTimeMillis = System.currentTimeMillis();
        startTimeNanos = System.nanoTime();
        thread = new Thread(this);
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
            conns = new ArrayList<PostgresServerConnection>(connections.values());
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
        logger.info("Postgres server listening on port {}", port);
        Random rand = new Random();
        try {
            reqs.monitor().registerServerMonitor(this);
            synchronized(this) {
                if (!running) return;
                socket = new ServerSocket(port);
                listening = true;
            }
            while (running) {
                Socket sock = socket.accept();
                int sessionId = reqs.monitor().allocateSessionId();
                int secret = rand.nextInt();
                PostgresServerConnection connection = 
                    new PostgresServerConnection(this, sock, sessionId, secret, reqs);
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
        return new ArrayList<PostgresServerConnection>(connections.values());
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

    void cleanStatementCaches() {
        long oldestGeneration = reqs.dxl().ddlFunctions().getOldestActiveGeneration();
        synchronized (statementCaches) {
            Iterator<ObjectLongPair> it = statementCaches.keySet().iterator();
            while(it.hasNext()) {
                if (it.next().longVal < oldestGeneration)
                    it.remove();
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
            statementCache = statementCaches.get(key);
            if (statementCache == null) {
                // No cache => recent DDL, reasonable time to do a little cleaning
                cleanStatementCaches();
                statementCache = new ServerStatementCache<PostgresStatement>(cacheCounters, statementCacheCapacity);
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
        return new HashSet<Integer>(connections.keySet());

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
    public long getStartTimeMillis() {
        return startTimeMillis;
    }
    
    @Override
    public int getSessionCount() {
        return nconnections;
    }

}
