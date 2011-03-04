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

package com.akiban.server.mttests.mthapi.base;

public final class WriteThreadStats {
    private final int writes;
    private final int updates;
    private final int deletes;

    public WriteThreadStats(int writes, int updates, int deletes) {
        this.writes = writes;
        this.updates = updates;
        this.deletes = deletes;
    }

    public int getWrites() {
        return writes;
    }

    public int getUpdates() {
        return updates;
    }

    public int getDeletes() {
        return deletes;
    }
}
