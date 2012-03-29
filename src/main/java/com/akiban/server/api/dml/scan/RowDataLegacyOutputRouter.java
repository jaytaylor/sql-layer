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

import com.akiban.server.rowdata.RowData;
import com.akiban.util.GrowableByteBuffer;

import java.util.ArrayList;
import java.util.List;

public final class RowDataLegacyOutputRouter implements LegacyRowOutput {
    public interface Handler {
        void mark();
        void rewind();
        void handleRow(RowData rowData);
    }

    private final List<Handler> handlers = new ArrayList<Handler>();
    private int rows = 0;
    private int markedRows = rows;

    /**
     * Adds a handler to this router. This method returns the handler back to you, as a convenience for when
     * you want to add a new handler and also keep a reference to it.
     * @param handler the handler to add to this router
     * @param <T> the handler's type
     * @return the handler you passed in
     */
    public <T extends Handler> T addHandler(T handler) {
        handlers.add(handler);
        return handler;
    }

    @Override
    public GrowableByteBuffer getOutputBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wroteRow(boolean limitExceeded) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRow(RowData rowData) {
        for(Handler handler : handlers) {
            handler.handleRow(rowData);
        }
        ++rows;
    }

    @Override
    public int getRowsCount() {
        return rows;
    }

    @Override
    public boolean getOutputToMessage() {
        return false;
    }

    @Override
    public void mark() {
        markedRows = rows;
        for(Handler handler : handlers) {
            handler.mark();
        }
    }

    @Override
    public void rewind() {
        rows = markedRows;
        for(Handler handler : handlers) {
            handler.rewind();
        }
    }
}
