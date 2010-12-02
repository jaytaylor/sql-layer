package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;

/**
 * <p>A class that acts as a LegacyRowOutput and converts each row, as it's seen, to a NiceRow. That NiceRow
 * is then forwarded to a RowOutput.</p>
 *
 * <p>As an example, let's say you have a RowOutput, but a method that takes a LegacyRowOutput. You would construct
 * a new LegacyOutputConverter, passing it your RowOutput, and then pass that converter to the method that takes
 * LegacyRowOutput.</p>
 */
public final class LegacyOutputConverter implements LegacyOutputRouter.Handler {
    private RowDef rowDef;
    private RowOutput output;

    @Override
    public void handleRow(byte[] bytes, int offset, int length) throws RowOutputException {
        RowData rowData = new RowData(bytes, offset, length);
        rowData.prepareRow(offset);
        NewRow aNew = NiceRow.fromRowData(rowData, rowDef);
        output.output(aNew);
    }

    public void setRowDef(RowDef rowDef) {
        this.rowDef = rowDef;
    }

    public void setOutput(RowOutput output) {
        this.output = output;
    }
}
