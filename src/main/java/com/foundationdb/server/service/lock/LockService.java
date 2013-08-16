/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.service.lock;

import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.session.Session;

public interface LockService extends Service {
    enum Mode { SHARED, EXCLUSIVE }

    boolean hasAnyClaims(Session session, Mode mode);
    boolean isTableClaimed(Session session, Mode mode, int tableID);

    void claimTable(Session session, Mode mode, int tableID);
    void claimTableInterruptible(Session session, Mode mode, int tableID) throws InterruptedException;

    boolean tryClaimTable(Session session, Mode mode, int tableID);
    boolean tryClaimTableMillis(Session session, Mode mode, int tableID, long milliseconds) throws InterruptedException;
    boolean tryClaimTableNanos(Session session, Mode mode, int tableID, long nanoseconds) throws InterruptedException;

    void releaseTable(Session session, Mode mode, int tableID);
}
