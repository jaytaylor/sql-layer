package com.akiban.ais.model;

import com.akiban.ais.model.ModelNames;

import java.util.Map;

public abstract class Target implements ModelNames
{
    public abstract void deleteAll() throws Exception;

    public abstract void writeCount(int count) throws Exception;


    public void writeType(Map<String, Object> map) throws Exception
    {
        write(type, map);
    }

    public final void writeGroup(Map<String, Object> map) throws Exception
    {
        write(group, map);
    }

    public final void writeTable(Map<String, Object> map) throws Exception
    {
        write(table, map);
    }

    public final void writeColumn(Map<String, Object> map) throws Exception
    {
        write(column, map);
    }

    public final void writeJoin(Map<String, Object> map) throws Exception
    {
        write(join, map);
    }

    public final void writeJoinColumn(Map<String, Object> map) throws Exception
    {
        write(joinColumn, map);
    }

    public final void writeIndex(Map<String, Object> map) throws Exception
    {
        write(index, map);
    }

    public final void writeIndexColumn(Map<String, Object> map) throws Exception
    {
        write(indexColumn, map);
    }

    protected abstract void write(final String string, final Map<String, Object> map) throws Exception;

    public abstract void close() throws Exception;
}