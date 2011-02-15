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

public interface Session
{
    <T> T get(Class<?> module, Object key);
    <T> T put(Class<?> module, Object key, T item);
    <T> T remove(Class<?> module, Object key);
    
    /**
     * A session may be marked as canceled.
     * 
     * For example, a connection may close and we no longer need to execute a long running
     * operation. Code that runs for a long time may look at the session periodically and then
     * stop its execution if the session is canceled.
     * 
     * @return true if the session is canceled, false otherwise.
     */
    boolean isCanceled();
    
    /**
     * Closes all the resources managed by this session.
     */
    void close();
}
