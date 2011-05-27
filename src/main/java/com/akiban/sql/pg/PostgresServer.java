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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.io.*;
import java.util.*;

/** The PostgreSQL server.
 * Listens of a given port and spawns <code>PostgresServerConnection</code> threads
 * to process requests.
 * Also keeps global state for shutdown and inter-connection communication like cancel.
*/
public class PostgresServer implements Runnable, PostgresMXBean {
    private int port;
    private PostgresStatementCache statementCache;
    private ServerSocket socket = null;
    private boolean running = false;
    private Map<Integer,PostgresServerConnection> connections =
        new HashMap<Integer,PostgresServerConnection>();
    private Thread thread;

    private static final Logger logger = LoggerFactory.getLogger(PostgresServer.class);

    public PostgresServer(int port, int statementCacheCapacity) {
        this.port = port;
        if (statementCacheCapacity > 0)
            statementCache = new PostgresStatementCache(statementCacheCapacity);
    }

    /** Called from the (AkServer's) main thread to start a server
        running in its own thread. */
    public void start() {
        running = true;
        thread = new Thread(this);
        thread.start();
    }

    /** Called from the main thread to shutdown a server. */
    public void stop() {
        ServerSocket socket;
        synchronized(this) {
            // Service might shutdown before we've even got server socket created.
            running = false;
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
            }
            catch (InterruptedException ex) {
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
            }
            while (running) {
                Socket sock = socket.accept();
                pid++;
                int secret = rand.nextInt();
                PostgresServerConnection connection = 
                    new PostgresServerConnection(this, sock, pid, secret);
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

    public synchronized PostgresServerConnection getConnection(int pid) {
        return connections.get(pid);
    }
   
    public synchronized void removeConnection(int pid) {
        connections.remove(pid);
    }

    @Override
    public synchronized Set<Integer> getCurrentConnections() {
        return new HashSet<Integer>(connections.keySet());
    }

    @Override
    public boolean isInstrumentationEnabled(int pid) {
        return getConnection(pid).isInstrumentationEnabled();
    }

    @Override
    public void enableInstrumentation(int pid) {
        getConnection(pid).enableInstrumentation();
    }
    
    @Override
    public void disableInstrumentation(int pid) {
        getConnection(pid).disableInstrumentation();
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

    /** This is the version for use by connections. */
    // TODO: This could create a new one if we didn't want to share them.
    public PostgresStatementCache getStatementCache(int generation) {
        if (statementCache != null)
            statementCache.checkGeneration(generation);
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
    
}
