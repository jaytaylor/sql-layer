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

package com.akiban.ais.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.akiban.ais.metamodel.Source;

public class ArraySource extends Source
{
    // Source interface

    @Override
    public void close()
    {
    }

    @Override 
    public int readVersion ()
    {
        return 0;
    }
    
    @Override
    protected final void read(String typename, Receiver receiver)
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