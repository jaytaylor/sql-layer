package com.foundationdb.server.test.it.qp;

import java.util.ArrayList;
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
        idCount++;
    }

    public List<Integer> ids()
    {
        return ids;
    }

    public void clear()
    {
        zToId.clear();
        ids.clear();
        idCount = 0;
    }

    public long[][] toArray(ExpectedRowCreator rowCreator)
    {
        return toArray(idCount, rowCreator);
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
        return array;
    }

    public interface ExpectedRowCreator
    {
        long[] fields(long z, int id);
    }

    private Map<Long, List<Integer>> zToId = new TreeMap<>();
    private List<Integer> ids = new ArrayList<>();
    private int idCount = 0;
}
