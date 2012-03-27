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

import java.nio.ByteBuffer;

import com.akiban.server.rowdata.RowData;
import com.akiban.util.ArgumentValidation;

public class WrappingRowOutput implements LegacyRowOutput {
    private int markPos = -1;
    protected final ByteBuffer wrapped;
    private int rows;

    /**
     * Creates a RowOutput that returns the given ByteBuffer
     * @param buffer the ByteBuffer to wrap
     * @throws IllegalArgumentException is buffer if null
     */
    public WrappingRowOutput(ByteBuffer buffer) {
        ArgumentValidation.notNull("buffer", buffer);
        this.wrapped = buffer;
    }

    @Override
    final public ByteBuffer getOutputBuffer() {
        return wrapped;
    }

    @Override
    final public void wroteRow(boolean limitExceeded) {
        if (!limitExceeded) {
            ++rows;
            postWroteRow();
        }
    }

    @Override
    public void addRow(RowData rowData)
    {
        throw new UnsupportedOperationException("Shouldn't be using addRow for output to a ScanRowsResponse message");
    }

    protected void postWroteRow() {

    }

    @Override
    final public int getRowsCount() {
        return rows;
    }

    @Override
    public boolean getOutputToMessage()
    {
        return true;
    }

    @Override
    public void mark() {
        markPos = wrapped.position();
    }

    @Override
    public void rewind() {
        wrapped.position(markPos);
    }
}
