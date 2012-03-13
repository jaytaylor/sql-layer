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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.IndexColumn;

import java.util.List;

public final class MultiIndexIntersectionScan extends IndexScan {
    
    private IndexScan outputScan;
    private IndexScan selectorScan;
    
    public MultiIndexIntersectionScan(IndexScan outputScan, IndexScan selectorScan) {
        super(outputScan.getRootMostTable(),
              outputScan.getRootMostInnerTable(),
              outputScan.getLeafMostInnerTable(),
              outputScan.getLeafMostTable());
        this.outputScan = outputScan;
        this.selectorScan = selectorScan;
    }

    public IndexScan getOutputScan() {
        return outputScan;
    }

    public IndexScan getSelectorScan() {
        return selectorScan;
    }

    @Override
    public List<IndexColumn> getKeyColumns() {
        return outputScan.getKeyColumns();
    }

    @Override
    public List<IndexColumn> getValueColumns() {
        return outputScan.getValueColumns();
    }

    @Override
    protected String summarizeIndex() {
        return String.format("INTERSECT(%s AND %s)", outputScan, selectorScan);
    }

    @Override
    protected boolean isAscendingAt(int index) {
        return outputScan.isAscendingAt(index);
    }
}
