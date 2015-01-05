/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.test.it.qp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class ZToIdMapping implements Iterable<Map.Entry<Long, List<Integer>>>
{
    @Override
    public Iterator<Map.Entry<Long, List<Integer>>> iterator()
    {
        return zToId.entrySet().iterator();
    }

    public int size()
    {
        return count;
    }

    public void add(long z, int id)
    {
        List<Integer> zIds = zToId.get(z);
        if (zIds == null) {
            zIds = new ArrayList<>();
            zToId.put(z, zIds);
        }
        assert !zIds.contains(id);
        zIds.add(id);
        ids.add(id);
        count++;
    }

    public List<Integer> ids()
    {
        return ids;
    }

    public void clear()
    {
        zToId.clear();
        ids.clear();
        count = 0;
    }

    public long[][] toArray(ExpectedRowCreator rowCreator)
    {
        return toArray(count, rowCreator);
    }

    public long[][] toArray(int expectedRows, ExpectedRowCreator rowCreator)
    {
        long[][] array = new long[expectedRows][];
        int r = 0;
        for (Map.Entry<Long, List<Integer>> entry : zToId.entrySet()) {
            long z = entry.getKey();
            for (Integer id : entry.getValue()) {
                long[] fields = rowCreator.fields(z, id);
                if (fields != null) {
                    array[r++] = fields;
                }
            }
        }
        return
            r == expectedRows
            ? array
            : Arrays.copyOf(array, r);
    }

    public interface ExpectedRowCreator
    {
        long[] fields(long z, int id);
    }

    private Map<Long, List<Integer>> zToId = new TreeMap<>();
    private List<Integer> ids = new ArrayList<>();
    private int count = 0;
}
