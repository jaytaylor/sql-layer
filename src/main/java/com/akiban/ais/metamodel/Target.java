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

public abstract class Target implements ModelNames
{
    public abstract void deleteAll();

    public abstract void writeCount(int count);

    public abstract void writeVersion(int modelVersion); 
    
    public void writeType(Map<String, Object> map) 
    {
        write(type, map);
    }

    public final void writeGroup(Map<String, Object> map) 
    {
        write(group, map);
    }

    public final void writeTable(Map<String, Object> map) 
    {
        write(table, map);
    }

    public final void writeColumn(Map<String, Object> map) 
    {
        write(column, map);
    }

    public final void writeJoin(Map<String, Object> map) 
    {
        write(join, map);
    }

    public final void writeJoinColumn(Map<String, Object> map) 
    {
        write(joinColumn, map);
    }

    public final void writeIndex(Map<String, Object> map) 
    {
        write(index, map);
    }

    public final void writeIndexColumn(Map<String, Object> map) 
    {
        write(indexColumn, map);
    }

    protected abstract void write(final String string, final Map<String, Object> map);

    public abstract void close();
}