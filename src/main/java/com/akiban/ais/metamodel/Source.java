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

package com.akiban.ais.metamodel;

import java.util.Map;

public abstract class Source implements ModelNames
{
    public void readTypes(Receiver typeReceiver)
    {
        read(type, typeReceiver);
    }

    public void readTables(Receiver tableReceiver)
    {
        read(table, tableReceiver);
    }

    public void readGroups(Receiver groupReceiver)
    {
        read(group, groupReceiver);
    }

    public void readColumns(Receiver columnReceiver)
    {
        read(column, columnReceiver);
    }

    public void readJoins(Receiver joinReceiver)
    {
        read(join, joinReceiver);
    }

    public void readJoinColumns(Receiver joinColumnReceiver)
    {
        read(joinColumn, joinColumnReceiver);
    }

    public void readIndexes(Receiver indexReceiver)
    {
        read(index, indexReceiver);
    }

    public void readIndexColumns(Receiver indexColumnReceiver)
    {
        read(indexColumn, indexColumnReceiver);
    }

    public abstract int readVersion ();
    
    protected abstract void read(final String typename, Receiver receiver);

    public abstract void close();

    public static abstract class Receiver implements ModelNames
    {
        public abstract void receive(Map<String, Object> map);
    }
}
