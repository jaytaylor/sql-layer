package com.akiban.ais.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.akiban.ais.model.Source;

public class ArraySource extends Source
{
    // Source interface

    @Override
    public void close()
    {
    }

    @Override
    protected final void read(String typename, Receiver receiver) throws Exception
    {
        for (Map<String, Object> map : columns) {
            receiver.receive(map);
        }
    }

    // ArraySource interface

    public void addColumn(Map<String, Object> map)
    {
        columns.add(map);
    }

    // State

    private final List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
}