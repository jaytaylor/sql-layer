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

package com.akiban.server.store;

import com.akiban.server.InvalidOperationException;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.*;

public abstract class IndexRecordVisitor
{
    public final void visit() throws PersistitException, InvalidOperationException
    {
        visit(key());
    }

    public final void initialize(Exchange exchange)
    {
        this.exchange = exchange;
    }

    public abstract void visit(List<Object> key);

    private List<Object> key()
    {
        // Key traversal
        Key key = exchange.getKey();
        int keySize = key.getDepth();
        List<Object> keyList = new ArrayList<Object>(keySize);
        for (int k = 0; k < keySize; k++) {
            keyList.add(key.decode());
        }
        return keyList;
    }

    protected Exchange exchange;
}
