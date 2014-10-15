/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.api.dml.scan;

import java.util.HashSet;
import java.util.Set;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.service.session.Session;

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
    private Session session;
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
        aNew = converter.convertRowData(session, rowData);

        if (columnsToScan != null) {
            final Set<Integer> colsToRemove = new HashSet<>();
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

    public void clearSession() {
        this.session = null;
    }

    public void reset(Session session, RowOutput output, Set<Integer> columns) {
        this.session = session;
        this.output = output;
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
