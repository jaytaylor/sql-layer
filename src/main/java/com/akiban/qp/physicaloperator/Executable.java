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

package com.akiban.qp.physicaloperator;

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.HKey;

import java.util.concurrent.atomic.AtomicInteger;

public class Executable
{
    public Executable(StoreAdapter adapter, PhysicalOperator root)
    {
        this.root = root;
        // Assign operator ids
        AtomicInteger idGenerator = new AtomicInteger(0);
        root.assignOperatorIds(idGenerator);
        int nOperators = idGenerator.get();
        // Instantiate plan
        ops = new OperatorExecution[nOperators];
        root.instantiate(adapter, ops);
    }

    public Executable bind(PhysicalOperator operator, IndexKeyRange keyRange)
    {
        ops[operator.operatorId()].bind(keyRange);
        return this;
    }

    public Executable bind(PhysicalOperator operator, HKey hKey)
    {
        ops[operator.operatorId()].bind(hKey);
        return this;
    }

    public Cursor cursor()
    {
        return ops[root.operatorId()];
    }

    // Object state

    private final PhysicalOperator root;
    private OperatorExecution[] ops;
}
