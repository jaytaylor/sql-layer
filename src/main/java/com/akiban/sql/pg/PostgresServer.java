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

package com.akiban.sql.pg;

import com.akiban.qp.loadableplan.LoadablePlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** The PostgreSQL server.
 * Listens of a given port and spawns <code>PostgresServerConnection</code> threads
 * to process requests.
 * Also keeps global state for shutdown and inter-connection communication like cancel.
*/
public class PostgresServer implements Runnable, PostgresMXBean {
    private final int port;
    private final PostgresServiceRequirements reqs;
    private ServerSocket socket = null;
    private boolean running = false;
    private volatile long startTime = 0;
    private boolean listening = false;
    private Map<Integer,PostgresServerConnection> connections =
        new HashMap<Integer,PostgresServerConnection>();
    private Thread thread;
    private final AtomicBoolean instrumentationEnabled = new AtomicBoolean(false);
    // AIS-dependent state
    private final Object aisLock = new Object();
    private volatile long aisTimestamp = -1;
    private volatile PostgresStatementCache statementCache;
    private final Map<String, LoadablePlan<?>> loadablePlans = new HashMap<String, LoadablePlan<?>>();
    // end AIS-dependent state
    private volatile Date overrideCurrentTime;

    private static final Logger logger = LoggerFactory.getLogger(PostgresServer.class);

    public PostgresServer(int port, int statementCacheCapacity, PostgresServiceRequirements reqs) {
        this.port = port;
        if (statementCacheCapacity > 0)
            statementCache = new PostgresStatementCache(statementCacheCapacity);
        this.reqs = reqs;
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

    public void run() {
        logger.debug("Postgres server listening on port {}", port);
        int pid = 0;
        Random rand = new Random();
        try {
            synchronized(this) {
                if (!running) return;
                socket = new ServerSocket(port);
                listening = true;
            }
            while (running) {
                Socket sock = socket.accept();
                pid++;
                int secret = rand.nextInt();
                PostgresServerConnection connection = 
                    new PostgresServerConnection(this, sock, pid, secret, reqs);
                connections.put(pid, connection);
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

    public synchronized PostgresServerConnection getConnection(int pid) {
        return connections.get(pid);
    }
   
    public synchronized void removeConnection(int pid) {
        connections.remove(pid);
    }
    
    @Override
    public String getSqlString(int pid) {
        return getConnection(pid).getSqlString();
    }
    
    @Override
    public String getRemoteAddress(int pid) {
        return getConnection(pid).getRemoteAddress();
    }

    public PostgresStatementCache getStatementCache() {
        return statementCache;
    }

    public LoadablePlan<?> loadablePlan(String planName) {
        return loadablePlans.get(planName);
    }

    /** This is the version for use by connections. */
    // TODO: This could create a new one if we didn't want to share them.
    public PostgresStatementCache getStatementCache(long timestamp)
    {
        synchronized (aisLock) {
            if (aisTimestamp != timestamp) {
                assert aisTimestamp < timestamp : timestamp;
                if (statementCache != null) {
                    statementCache.invalidate();
                }
                clearPlans();
                aisTimestamp = timestamp;
            }
        }
        return statementCache;
    }

    @Override
    public int getStatementCacheCapacity() {
        if (statementCache == null)
            return 0;
        else
            return statementCache.getCapacity();
    }

    @Override
    public void setStatementCacheCapacity(int capacity) {
        if (capacity <= 0) {
            statementCache = null;
        }
        else if (statementCache == null)
            statementCache = new PostgresStatementCache(capacity);
        else
            statementCache.setCapacity(capacity);
    }

    @Override
    public int getStatementCacheHits() {
        if (statementCache == null)
            return 0;
        else
            return statementCache.getHits();
    }

    @Override
    public int getStatementCacheMisses() {
        if (statementCache == null)
            return 0;
        else
            return statementCache.getMisses();
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

    @Override
    public void clearPlans()
    {
        loadablePlans.clear();
        loadInitialPlans();
    }

    // (Re-)load any initial plans.
    private void loadInitialPlans() {
        String plans = reqs.config().getProperty("akserver.postgres.loadablePlans");
        if (plans.length() > 0) {
            for (String className : plans.split(",")) {
                try {
                    Class klass = Class.forName(className);
                    LoadablePlan<?> loadablePlan = (LoadablePlan)klass.newInstance();
                    LoadablePlan<?> prev = loadablePlans.put(loadablePlan.name(), loadablePlan);
                    assert (prev == null) : className;
                }
                catch (ClassNotFoundException ex) {
                    logger.error("Failed to load plan", ex);
                }
                catch (InstantiationException ex) {
                    logger.error("Failed to create plan", ex);
                }
                catch (IllegalAccessException ex) {
                    logger.error("Failed to create plan", ex);
                }
            }
        }
    }

    @Override
    public String loadPlan(String jarFilePath, String className) {
        String status;
        try {
            File jarFile = new File(jarFilePath);
            if (!jarFile.isAbsolute()) {
                throw new IOException(String.format("jar file name does not specify an absolute path: %s",
                                                    jarFilePath));
            }
            URL url = new URL(String.format("file://%s", jarFilePath));
            URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
            Class klass = classLoader.loadClass(className);
            LoadablePlan<?> loadablePlan = (LoadablePlan) klass.newInstance();
            LoadablePlan<?> previousPlan = loadablePlans.put(loadablePlan.name(), loadablePlan);
            status = String.format("%s %s -> %s",
                                   (previousPlan == null ? "Loaded" : "Reloaded"),
                                   loadablePlan.name(),
                                   className);
        } catch (Exception e) {
            status = e.toString();
        }
        return status;
    }

    /** For testing, set the server's idea of the current time. */
    public void setOverrideCurrentTime(Date overrideCurrentTime) {
        this.overrideCurrentTime = overrideCurrentTime;
    }

    public Date getOverrideCurrentTime() {
        return overrideCurrentTime;
    }
}
