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

import com.akiban.server.service.Service;

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
public class PostgresServer implements Runnable {
    public static final int DEFAULT_PORT = 15432; // Real one is 5432
    
    private int port = DEFAULT_PORT;
    private ServerSocket socket = null;
    private boolean running = false;
    private Map<Integer,PostgresServerConnection> connections =
        new HashMap<Integer,PostgresServerConnection>();

    private static final Logger logger = LoggerFactory.getLogger(PostgresServer.class);

    public PostgresServer(int port) {
        this.port = port;
    }

    /** Called from the (AkServer's) main thread to start a server
        running in its own thread. */
    public void start() {
        running = true;
        new Thread(this).start();
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

        for (PostgresServerConnection connection : connections.values()) {
            connection.stop();
        }
    }

    public void run() {
        logger.warn("Postgres server listening on port {}", port);
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

    public PostgresServerConnection getConnection(int pid) {
        return connections.get(pid);
    }
    public void removeConnection(int pid) {
        connections.remove(pid);
    }

}
