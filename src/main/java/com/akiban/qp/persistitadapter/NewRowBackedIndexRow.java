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

import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;

public class NewRowBackedIndexRow implements RowBase
{
    // Object interface

    @Override
    public String toString()
    {
        return row.toString();
    }

    // NewRowBackedIndexRow interface

    NewRowBackedIndexRow(RowType rowType, NewRow row, TableIndex index) {
        this.rowType = rowType;
        this.row = row;
        this.index = index;
        this.sources = new FromObjectValueSource[rowType.nFields()];
        for (int f = 0; f < rowType.nFields(); f++) {
            this.sources[f] = new FromObjectValueSource();
        }
    }

    // BoundExpressions interface

    @Override
    public int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        throw new UnsupportedOperationException();
    }

    // RowBase interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        FieldDef fieldDef = index.getAllColumns().get(i).getColumn().getFieldDef();
        int fieldPos = fieldDef.getFieldIndex();
        FromObjectValueSource source = sources[fieldPos];
        if (row.isColumnNull(fieldPos)) {
            source.setNull();
        } else {
            source.setReflectively(row.get(fieldPos));
        }
        return source;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HKey ancestorHKey(UserTable table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ancestorOf(RowBase that) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    // Object state

    private final NewRow row;
    private final RowType rowType;
    private final TableIndex index;
    private final FromObjectValueSource[] sources;
}
