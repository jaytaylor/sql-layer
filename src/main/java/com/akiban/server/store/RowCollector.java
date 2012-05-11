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

package com.akiban.server.store;

import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.util.GrowableByteBuffer;

public interface RowCollector {

    public final int SCAN_FLAGS_DESCENDING = 1 << 0;

    public final int SCAN_FLAGS_START_EXCLUSIVE = 1 << 1;

    public final int SCAN_FLAGS_END_EXCLUSIVE = 1 << 2;

    public final int SCAN_FLAGS_SINGLE_ROW = 1 << 3;

    public final int SCAN_FLAGS_LEXICOGRAPHIC = 1 << 4;

    public final int SCAN_FLAGS_START_AT_EDGE = 1 << 5;

    public final int SCAN_FLAGS_END_AT_EDGE = 1 << 6;

    public final int SCAN_FLAGS_DEEP = 1 << 7;

    /**
     * Place the next row into payload if there is another row, and if there is room in payload.
     *
     * @param payload
     * @return true if a row was placed into payload, false otherwise
     * @throws Exception
     */
    public boolean collectNextRow(GrowableByteBuffer payload);

    public RowData collectNextRow();

    public boolean hasMore();

    public void open();

    public void close();
    
    public int getDeliveredRows();

    public int getDeliveredBuffers();
    
    public long getDeliveredBytes();
    
    public int getTableId();

    public IndexDef getIndexDef();

    public long getId();

    public void outputToMessage(boolean outputToMessage);

    public boolean checksLimit();
}
