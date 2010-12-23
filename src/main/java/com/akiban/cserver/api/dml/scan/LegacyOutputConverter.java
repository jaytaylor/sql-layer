package com.akiban.cserver.api.dml.scan;

import java.util.HashSet;
import java.util.Set;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.NoSuchTableException;

/**
 * <p>A class that acts as a LegacyRowOutput and converts each row, as it's seen, to a NiceRow. That NiceRow
 * is then forwarded to a RowOutput.</p>
 *
 * <p>As an example, let's say you have a RowOutput, but a method that takes a LegacyRowOutput. You would construct
 * a new LegacyOutputConverter, passing it your RowOutput, and then pass that converter to the method that takes
 * LegacyRowOutput.</p>
 */
public final class LegacyOutputConverter implements LegacyOutputRouter.Handler {
    private final DMLFunctions converter;
    private RowOutput output;
    private Set<ColumnId> columnsToScan;

    public LegacyOutputConverter(DMLFunctions converter) {
        this.converter = converter;
    }

    @Override
    public void handleRow(byte[] bytes, int offset, int length) throws RowOutputException {
        RowData rowData = new RowData(bytes, offset, length);
        rowData.prepareRow(offset);
        final NewRow aNew;
        try {
            aNew = converter.convertRowData(rowData);
        } catch (NoSuchTableException e) {
            throw new RowOutputException(e);
        }

        if (columnsToScan != null) {
            final Set<ColumnId> colsToRemove = new HashSet<ColumnId>();
            for (ColumnId newRowCol : aNew.getFields().keySet()) {
                if (!columnsToScan.contains(newRowCol)) {
                    colsToRemove.add(newRowCol);
                }
            }
            for (ColumnId removeMe : colsToRemove) {
                aNew.remove(removeMe);
            }
        }

        output.output(aNew);
    }

    public void setOutput(RowOutput output) {
        this.output = output;
    }

    public void setColumnsToScan(Set<ColumnId> columns) {
        this.columnsToScan = columns;
    }
}
