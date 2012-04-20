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
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.session.Session;
import com.akiban.util.GrowableByteBuffer;
import com.akiban.util.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RowDataOutput implements LegacyRowOutput {
    private final static Logger LOG = LoggerFactory.getLogger(RowDataOutput.class);

    private final List<RowData> rowDatas = new ArrayList<RowData>();
    private int markedRows = 0;

    public List<RowData> getRowDatas() {
        return rowDatas;
    }

    @Override
    public GrowableByteBuffer getOutputBuffer() {
        return null;
    }

    @Override
    public void wroteRow(boolean limitExceeded) {
        throw new UnsupportedOperationException("Shouldn't be calling wroteRow for output to an HAPI request");
    }

    @Override
    public void addRow(RowData rowData)
    {
        rowDatas.add(rowData);
    }

    @Override
    public int getRowsCount() {
        return rowDatas.size();
    }

    @Override
    public boolean getOutputToMessage()
    {
        return false;
    }

    @Override
    public void mark() {
        markedRows = rowDatas.size();
    }

    @Override
    public void rewind() {
        ListUtils.truncate(rowDatas, markedRows);
    }

    /**
     * Convenience method for doing a full scan (that is, a scan until there are no more rows to be scanned for the
     * request) and returning the rows.
     *
     *
     *
     * @param session the session in which to scan
     * @param dml the DMLFunctions to handle the scan
     * @param request the scan request
     * @param knownAIS the AIS generation the caller knows about
     * @return the resulting rows
     * @throws InvalidOperationException if thrown at any point during the scan
     */
    public static List<RowData> scanFull(Session session, int knownAIS, DMLFunctions dml, ScanRequest request)
            throws InvalidOperationException
    {
        final RowDataOutput output = new RowDataOutput();
        CursorId scanCursor = dml.openCursor(session, knownAIS, request);
        try {
            dml.scanSome(session, scanCursor, output);
            return output.getRowDatas();
        } catch (BufferFullException e) {
            LOG.error(String.format("This is unexpected, request: %s", request), e);
            assert false : request;
            return null;
        } finally {
            dml.closeCursor(session, scanCursor);
        }
    }
}
