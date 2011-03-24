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

import java.util.SortedMap;
import java.util.TreeMap;

public class TableStatusAccumulator {

    public static boolean selected(final String volumeName,
            final String treeName) {
        if (volumeName.contains("_txn") || volumeName.contains("_sys")) {
            return false;
        }
        if (treeName.startsWith("_") && treeName.endsWith("_")) {
            return false;
        }
        if (treeName.contains("&&")) {
            return false;
        }
        return true;
    }

    final int tableId;
    
    long deltaRowCount;
    
    final SortedMap<Long, RowData> recentRowDataMap = new TreeMap<Long, RowData>();
    
    public TableStatusAccumulator(final int tableid) {
        this.tableId = tableid;
    }

    public void insert(final long timestamp, final byte[] rowDataBytes,
            final int offset, final int length) {
        deltaRowCount++;
    }

    public void update(final long timestamp, final byte[] rowDataBytes,
            final int offset, final int length) {
        
    }
    
    public int getTableId() {
        return tableId;
    }
    
    public long getDeltaRowCount() {
        return deltaRowCount;
    }
}
