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

package com.akiban.server;

import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Accumulator;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

public class AccumulatorHandler {

    public long getSnapshot() throws PersistitInterruptedException {
        return accumulator.getSnapshotValue(getCurrentTrx());
    }

    public long update(long value) {
        return accumulator.update(value, getCurrentTrx());
    }

    public AccumulatorHandler(TreeService treeService, AccumInfo accumInfo, Tree tree) {
        this.treeService = treeService;
        this.accumulator = getAccumulator(accumInfo, tree);
    }

    private Accumulator getAccumulator(AccumInfo accumInfo, Tree tree) {
        try {
            return tree.getAccumulator(accumInfo.getType(), accumInfo.getIndex());
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    private Transaction getCurrentTrx() {
        return treeService.getDb().getTransaction();
    }

    private final TreeService treeService;
    private final Accumulator accumulator;
}
