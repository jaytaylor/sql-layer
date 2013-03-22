
package com.akiban.server.api.dml.scan;

import java.util.HashSet;
import java.util.Set;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.service.session.Session;

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
