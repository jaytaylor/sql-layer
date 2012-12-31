/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
