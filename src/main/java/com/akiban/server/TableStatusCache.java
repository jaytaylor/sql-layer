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

package com.akiban.server;

public interface TableStatusCache {
    void rowDeleted(int tableID);

    void rowUpdated(int tableID);

    void rowWritten(int tableID);

    void truncate(int tableID);

    void drop(int tableID);

    void setAutoIncrement(int tableID, long value);

    long createNewUniqueID(int tableID);
    void setUniqueID(int tableID, long value);

    void setOrdinal(int tableID, int value);

    TableStatus getTableStatus(int tableID);

    void detachAIS();
}
