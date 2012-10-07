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

import com.akiban.sql.server.ServerServiceRequirements;
import com.akiban.sql.server.ServerStatementCache;

import com.akiban.server.error.InvalidPortException;

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
public class PostgresServer implements Runnable, PostgresMXBean {
    public static final String SERVER_PROPERTIES_PREFIX = "akserver.postgres.";

    private final Properties properties;
    private final int port;
    private final ServerServiceRequirements reqs;
    private ServerSocket socket = null;
    private volatile boolean running = false;
    private volatile long startTime = 0;
    private boolean listening = false;
    private Map<Integer,PostgresServerConnection> connections =
        new HashMap<Integer,PostgresServerConnection>();
    private Thread thread;
    private final AtomicBoolean instrumentationEnabled = new AtomicBoolean(true);
    // AIS-dependent state
    private volatile long aisTimestamp = -1;
    private volatile int statementCacheCapacity;
    private final Map<Object,ServerStatementCache<PostgresStatement>> statementCaches = new HashMap<Object,ServerStatementCache<PostgresStatement>>();
    // end AIS-dependent state
    private volatile Date overrideCurrentTime;

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
        startTime = System.nanoTime();
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
        int sessionId = 0;
        Random rand = new Random();
        try {
            synchronized(this) {
                if (!running) return;
                socket = new ServerSocket(port);
                listening = true;
            }
            while (running) {
                Socket sock = socket.accept();
                sessionId++;
                int secret = rand.nextInt();
                PostgresServerConnection connection = 
                    new PostgresServerConnection(this, sock, sessionId, secret, reqs);
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
        return getConnection(sessionId).getSqlString();
    }
    
    @Override
    public String getRemoteAddress(int sessionId) {
        return getConnection(sessionId).getRemoteAddress();
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

    public ServerStatementCache<PostgresStatement> getStatementCache(Object key) {
        if (statementCacheCapacity <= 0) 
            return null;

        ServerStatementCache<PostgresStatement> statementCache;
        synchronized (statementCaches) {
            statementCache = statementCaches.get(key);
            if (statementCache == null) {
                statementCache = new ServerStatementCache<PostgresStatement>(statementCacheCapacity);
                statementCaches.put(key, statementCache);
            }
        }
        return statementCache;
    }

    /** This is the version for use by connections. */
    public ServerStatementCache<PostgresStatement> getStatementCache(Object key, long timestamp) {
        synchronized (statementCaches) {
            if (aisTimestamp != timestamp) {
                assert aisTimestamp < timestamp : timestamp;
                for (ServerStatementCache<PostgresStatement> statementCache : statementCaches.values()) {
                    statementCache.invalidate();
                }
                aisTimestamp = timestamp;
            }
        }
        return getStatementCache(key);
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
        int total = 0;
        synchronized (statementCaches) {
            for (ServerStatementCache<PostgresStatement> statementCache : statementCaches.values()) {
                total += statementCache.getHits();
            }        
        }
        return total;
    }

    @Override
    public int getStatementCacheMisses() {
        int total = 0;
        synchronized (statementCaches) {
            for (ServerStatementCache<PostgresStatement> statementCache : statementCaches.values()) {
                total += statementCache.getMisses();
            }        
        }
        return total;
    }
    
    @Override
    public void resetStatementCache() {
        synchronized (statementCaches) {
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
    public boolean isInstrumentationEnabled() {
        return instrumentationEnabled.get();
    }

    @Override
    public void enableInstrumentation() {
        for (PostgresServerConnection conn : connections.values()) {
            conn.getSessionTracer().enable();
        }
        instrumentationEnabled.set(true);
    }

    @Override
    public void disableInstrumentation() {
        for (PostgresServerConnection conn : connections.values()) {
            conn.getSessionTracer().disable();
        }
        instrumentationEnabled.set(false);
    }

    @Override
    public boolean isInstrumentationEnabled(int sessionId) {
        return getConnection(sessionId).isInstrumentationEnabled();
    }

    @Override
    public void enableInstrumentation(int sessionId) {
        getConnection(sessionId).enableInstrumentation();
    }

    @Override
    public void disableInstrumentation(int sessionId) {
        getConnection(sessionId).disableInstrumentation();
    }

    @Override
    public Date getStartTime(int sessionId) {
        return getConnection(sessionId).getSessionTracer().getStartTime();
    }

    @Override
    public long getProcessingTime(int sessionId) {
        return getConnection(sessionId).getSessionTracer().getProcessingTime();
    }

    @Override
    public long getEventTime(int sessionId, String eventName) {
        return getConnection(sessionId).getSessionTracer().getEventTime(eventName);
    }

    @Override
    public long getTotalEventTime(int sessionId, String eventName) {
        return getConnection(sessionId).getSessionTracer().getTotalEventTime(eventName);
    }

    @Override
    public long getUptime()
    {
        return (System.nanoTime() - startTime);
    }

    /** For testing, set the server's idea of the current time. */
    public void setOverrideCurrentTime(Date overrideCurrentTime) {
        this.overrideCurrentTime = overrideCurrentTime;
    }

    public Date getOverrideCurrentTime() {
        return overrideCurrentTime;
    }
}
