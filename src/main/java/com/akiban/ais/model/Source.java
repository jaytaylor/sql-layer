package com.akiban.ais.model;

import java.util.Map;

public abstract class Source implements ModelNames
{
    public void readTypes(Receiver typeReceiver) throws Exception
    {
        read(type, typeReceiver);
    }

    public void readTables(Receiver tableReceiver) throws Exception
    {
        read(table, tableReceiver);
    }

    public void readGroups(Receiver groupReceiver) throws Exception
    {
        read(group, groupReceiver);
    }

    public void readColumns(Receiver columnReceiver) throws Exception
    {
        read(column, columnReceiver);
    }

    public void readJoins(Receiver joinReceiver) throws Exception
    {
        read(join, joinReceiver);
    }

    public void readJoinColumns(Receiver joinColumnReceiver) throws Exception
    {
        read(joinColumn, joinColumnReceiver);
    }

    public void readIndexes(Receiver indexReceiver) throws Exception
    {
        read(index, indexReceiver);
    }

    public void readIndexColumns(Receiver indexColumnReceiver) throws Exception
    {
        read(indexColumn, indexColumnReceiver);
    }

    protected abstract void read(final String typename, Receiver receiver) throws Exception;

    public abstract void close() throws Exception;

    public static abstract class Receiver implements ModelNames
    {
        public abstract void receive(Map<String, Object> map) throws Exception;
    }
}
