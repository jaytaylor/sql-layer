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

package com.akiban.server.test.it.hapi.randomdb;

import com.akiban.server.api.dml.scan.NewRow;

import static org.junit.Assert.fail;

class HKey implements Comparable<HKey>
{
    @Override
    public int compareTo(HKey that)
    {
        int n = Math.min(this.key.length, that.key.length);
        for (int i = 0; i < n; i++) {
            long c = this.key[i] - that.key[i];
            if (c != 0) {
                return (int) c;
            }
        }
        return (this.key.length - that.key.length);
    }

    public HKey(RCTortureIT test, NewRow row)
    {
        int tableId = row.getTableId();
        if (tableId == test.customerTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0)
            };
        } else if (tableId == test.orderTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0),
                test.orderTable, (Long) row.get(1)
            };
        } else if (tableId == test.itemTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0),
                test.orderTable, (Long) row.get(1),
                test.itemTable, (Long) row.get(2)
            };
        } else if (tableId == test.addressTable) {
            key = new long[]{
                test.customerTable, (Long) row.get(0),
                test.addressTable, (Long) row.get(1)
            };
        } else {
            key = null;
            fail();
        }
    }

    public int differSegment(HKey that)
    {
        int differSegment;
        int nx = this.key.length;
        int ny = that.key.length;
        int n = Math.min(nx, ny);
        for (differSegment = 0; differSegment < n; differSegment++) {
            if (this.key[differSegment] != that.key[differSegment]) {
                break;
            }
        }
        return differSegment;
    }

    private final long[] key;
}
