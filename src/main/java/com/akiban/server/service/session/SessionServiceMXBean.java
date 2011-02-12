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

public interface SessionServiceMXBean {
    /** Total number of created sessions. This is <em>not</em> incremented atomically with token creation,
     * so it should be considered an estimate. However, it's safe to assume that this number monotonically increases.
     */
    int getSessionsCreated();

    /** Number of sessions  that have been closed. This is <em>not</em> incremented atomically with token closure,
     * so it should be considered an estimate. However, it's safe to assume that this number monotonically increases.
     */
    int getSessionsClosed();

    /** Total number of created sessions. This is <em>not</em> incremented atomically with token activation
     * or removal, so it should be considered an estimate.
     */
    int getSessionsActive();
}
