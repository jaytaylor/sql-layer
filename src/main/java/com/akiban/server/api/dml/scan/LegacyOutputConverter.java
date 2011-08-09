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

package com.akiban.server.api.dml.scan;

import java.util.HashSet;
import java.util.Set;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.RowOutputException;

/**
 * <p>A class that acts as a LegacyRowOutput and converts each row, as it's seen, to a NiceRow. That NiceRow
 * is then forwarded to a RowOutput.</p>
 *
 * <p>As an example, let's say you have a RowOutput, but a method that takes a LegacyRowOutput. You would construct
 * a new LegacyOutputConverter, passing it your RowOutput, and then pass that converter to the method that takes
 * LegacyRowOutput.</p>
 */
public final class LegacyOutputConverter implements BufferedLegacyOutputRouter.Handler {
    private final DMLFunctions converter;
    private RowOutput output;
    private Set<Integer> columnsToScan;

    public LegacyOutputConverter(DMLFunctions converter) {
        this.converter = converter;
    }

    @Override
    public void handleRow(byte[] bytes, int offset, int length) {
        RowData rowData = new RowData(bytes, offset, length);
        rowData.prepareRow(offset);
        final NewRow aNew;
        //try {
            aNew = converter.convertRowData(rowData);
        //} catch (NoSuchTableException e) {
        //    throw new RowOutputException(e);
        //}

        if (columnsToScan != null) {
            final Set<Integer> colsToRemove = new HashSet<Integer>();
            for (Integer newRowCol : aNew.getFields().keySet()) {
                if (!columnsToScan.contains(newRowCol)) {
                    colsToRemove.add(newRowCol);
                }
            }
            for (Integer removeMe : colsToRemove) {
                aNew.remove(removeMe);
            }
        }

        output.output(aNew);
    }

    public void setOutput(RowOutput output) {
        this.output = output;
    }

    public void setColumnsToScan(Set<Integer> columns) {
        this.columnsToScan = columns;
    }

    @Override
    public void mark() {
        output.mark();
    }

    @Override
    public void rewind() {
        output.rewind();
    }
}
