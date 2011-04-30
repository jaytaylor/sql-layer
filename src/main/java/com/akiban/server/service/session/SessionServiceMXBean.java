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

@SuppressWarnings("unused") // jmx
public interface SessionServiceMXBean {
    long getCreated();
    long getGCed();
    long getClosed();

    /**
     * Gets a rough sense of how many sessions are opened. This method is very rough; it makes no synchronization
     * guarantees. For instance an implementation that invokes {@code getCreated() - (getGCed() - getClosed())}, with no
     * atomicity between each of those methods, would be acceptable.
     * @return roughly the number of open sessions
     */
    long getOpenedEstimate();
}
