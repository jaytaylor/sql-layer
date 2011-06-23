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

package com.akiban.server.test.it.keyupdate;

import com.akiban.server.store.IndexKeyRecordVisitor;

import java.util.ArrayList;
import java.util.List;

public class CollectingIndexKeyVisitor extends IndexKeyRecordVisitor
{
    @Override
    protected void visit(List<?> key)
    {
        records.add(key);
    }

    public List<List<?>> records()
    {
        return records;
    }

    private final List<List<?>> records = new ArrayList<List<?>>();
}