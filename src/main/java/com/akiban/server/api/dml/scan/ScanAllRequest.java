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

import java.util.EnumSet;
import java.util.Set;

public class ScanAllRequest extends ScanAllRange implements ScanRequest {
    private final int indexId;
    private final int scanFlags;
    private ScanLimit limit;

    public ScanAllRequest(int tableId, Set<Integer> columnIds) {
        this(tableId, columnIds, 0, null);
    }

    public ScanAllRequest(int tableId,
                          Set<Integer> columnIds,
                          int indexId,
                          EnumSet<ScanFlag> scanFlags,
                          ScanLimit limit)
    {
        super(tableId, columnIds);
        this.indexId = indexId;
        this.scanFlags = ScanFlag.toRowDataFormat(scanFlags != null ? scanFlags : EnumSet.noneOf(ScanFlag.class));
        this.limit = limit;
    }
    public ScanAllRequest(int tableId,
                          Set<Integer> columnIds,
                          int indexId,
                          EnumSet<ScanFlag> scanFlags)
    {

        this(tableId, columnIds, indexId, scanFlags, ScanLimit.NONE);
    }

    @Override
    public int getIndexId() {
        return indexId;
    }

    @Override
    public int getScanFlags() {
        return scanFlags;
    }

    @Override
    public ScanLimit getScanLimit() {
        return limit;
    }

    @Override
    public void dropScanLimit()
    {
        limit = ScanLimit.NONE;
    }
}
