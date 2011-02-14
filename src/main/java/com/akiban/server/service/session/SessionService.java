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

package com.akiban.server.service.session;

/**
 * Session and session lifecycle management service.
 *
 * <p>A SessionService is responsible for creating Sessions, which it then owns but can rent out to other classes
 * via {@link #acquireSession(SessionHandle)}. SessionServices are required to maintain the invariant that each
 * SessionHandle's Session can only be acquired once at a time. They are also required to make acquisition and
 * release of Sessions synchronous. This means that as long as a reference to an acquired Session is confined to its
 * thread and is not kept after the session is released, no additional sychronization is required, either externally
 * (by the Session's users) or internally to the Session itself.</p>
 *
 * <p>Example use case:</p>
 *
 * <pre style='border 1px solid gray'>
 * // Network layer:
 * void onConnect(Connection connection) {
 *      SessionHandle handle = new ConnectionSessionHandle(connection);
 *      sessionService.createSession( handle );
 * }
 *
 * void onDisconnect(Connection connection) {
 *      SessionHandle handle = new ConnectionSessionHandle(connection); // Could also look it up in a Map, etc
 *      sessionService.destroySession( handle );
 * }
 *
 * void incomingRequest(Connection connection, Request request) {
 *      SessionHandle handle = new ConnectionSessionHandle(connection);
 *      try {
 *          execute(request, sessionService.acquireSession( handle );
 *      }
 *      finally {
 *          sessionService.releaseSession( handle );
 *      }
 * }
 * </pre>
 */
public interface SessionService
{
    public SessionHandle createSessionWithHandle() throws SessionException;
    public void createSession(SessionHandle sessionHandle) throws SessionException;

    public Session acquireSession(SessionHandle sessionHandle) throws SessionException;
    public void releaseSession(SessionHandle sessionHandle) throws SessionException;
    
    public void destroySession(SessionHandle sessionHandle) throws SessionException;
}
