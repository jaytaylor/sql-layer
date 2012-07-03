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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowDataPValueSource;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.SparseArray;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// public for access by OperatorIT
public class PersistitGroupRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return rowData == null ? null : rowData.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return adapter.schema().userTableRowType(rowDef().userTable());
    }

    @Override
    public ValueSource eval(int i)
    {
        FieldDef fieldDef = rowDef().getFieldDef(i);
        RowData rowData = rowData();
        RowDataValueSource valueSource = valueSource(i);
        valueSource.bind(fieldDef, rowData);
        return valueSource;
    }

    @Override
    public PValueSource pvalue(int i) {
        FieldDef fieldDef = rowDef().getFieldDef(i);
        RowData rowData = rowData();
        RowDataPValueSource valueSource = pValueSource(i);
        valueSource.bind(fieldDef, rowData);
        return valueSource;
    }

    @Override
    public PersistitHKey hKey()
    {
        return currentHKey;
    }

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        PersistitHKey ancestorHKey = hKeyCache.hKey(table);
        currentHKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable)
    {
        return row.getRowDef().userTable() == userTable;
    }

    // PersistitGroupRow interface

    static PersistitGroupRow newPersistitGroupRow(PersistitAdapter adapter)
    {
        return new PersistitGroupRow(adapter);
    }

    // For use by OperatorIT
    public static PersistitGroupRow newPersistitGroupRow(PersistitAdapter adapter, RowData rowData)
    {
        return new PersistitGroupRow(adapter, rowData);
    }

    // For use by this package

    RowDef rowDef()
    {
        if (row != null) {
            return row.getRowDef();
        }
        if (rowData != null) {
            return adapter.rowDef(rowData.getRowDefId());
        }
        throw new IllegalStateException("no active row");
    }

    void copyFromExchange(Exchange exchange) throws PersistitException
    {
        this.row = new LegacyRowWrapper((RowDef) null);
        RuntimeException exception;
        do {
            try {
                exception = null;
                adapter.persistit().expandRowData(exchange, rowData);
                row.setRowDef(rowData.getRowDefId(), adapter.persistit());
                row.setRowData(rowData);
                PersistitHKey persistitHKey = persistitHKey();
                persistitHKey.copyFrom(exchange.getKey());
                rowData.hKey(persistitHKey.key());
            } catch (ArrayIndexOutOfBoundsException e) {
                exception = e;
            } catch (EncodingException e) {
                if (e.getCause() instanceof ArrayIndexOutOfBoundsException) {
                    exception = e;
                } else {
                    throw e;
                }
            }
            if (exception != null) {
                int newSize = rowData.getBytes().length * 2;
                if (newSize >= MAX_ROWDATA_SIZE_BYTES) {
                    LOG.error("{}: Unable to copy from exchange for key {}: {}",
                              new Object[] {this, exchange.getKey(), exception.getMessage()});
                    throw exception;
                }
                rowData.reset(new byte[newSize]);
            }
        } while (exception != null);
    }

    public RowData rowData()
    {
        return rowData;
    }

    // For use by this class

    private PersistitHKey persistitHKey()
    {
        currentHKey = hKeyCache.hKey(row.getRowDef().userTable());
        return currentHKey;
    }

    private PersistitGroupRow(PersistitAdapter adapter)
    {
        this(adapter, new RowData(new byte[INITIAL_ROW_SIZE]));
    }

    private PersistitGroupRow(PersistitAdapter adapter, RowData rowData)
    {
        this.adapter = adapter;
        this.rowData = rowData;
        this.hKeyCache = new HKeyCache(adapter);
    }

    private RowDataValueSource valueSource(int i)
    {
        if (valueSources == null) {
            valueSources = new SparseArray<RowDataValueSource>()
            {
                @Override
                protected RowDataValueSource initialValue()
                {
                    return new RowDataValueSource();
                }
            };
        }
        return valueSources.get(i);
    }

    private RowDataPValueSource pValueSource(int i) {
        if (pvalueSources == null) {
            pvalueSources = new SparseArray<RowDataPValueSource>()
            {
                @Override
                protected RowDataPValueSource initialValue() {
                    return new RowDataPValueSource();
                }
            };
        }
        return pvalueSources.get(i);
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(PersistitGroupRow.class);
    private static final int INITIAL_ROW_SIZE = 500;
    private static final int MAX_ROWDATA_SIZE_BYTES = 5000000;

    // Object state

    private SparseArray<RowDataValueSource> valueSources;
    private SparseArray<RowDataPValueSource> pvalueSources;
    private final PersistitAdapter adapter;
    private RowData rowData;
    private LegacyRowWrapper row;
    private PersistitHKey currentHKey;
    private HKeyCache<PersistitHKey> hKeyCache;
}
