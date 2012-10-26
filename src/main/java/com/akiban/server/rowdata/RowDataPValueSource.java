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

package com.akiban.server.rowdata;

import com.akiban.server.types3.pvalue.PValueSource;

public final class RowDataPValueSource extends AbstractRowDataPValueSource implements RowDataSource {

    // RowDataSource interface

    @Override
    public void bind(FieldDef fieldDef, RowData rowData) {
        this.fieldDef = fieldDef;
        this.rowData = rowData;
    }

    // AbstractRowDataValueSource interface

    @Override
    protected long getRawOffsetAndWidth() {
        return fieldDef().getRowDef().fieldLocation(rowData(), fieldDef().getFieldIndex());
    }

    @Override
    protected byte[] bytes() {
        return rowData.getBytes();
    }

    @Override
    protected FieldDef fieldDef() {
        return fieldDef;
    }

    // ValueSource interface

    @Override
    public boolean isNull() {
        return (rowData().isNull(fieldDef().getFieldIndex()));
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("ValueSource( %s -> %s )", fieldDef, rowData.toString(fieldDef.getRowDef()));
    }

    // private
    
    private RowData rowData() {
        return rowData;
    }

    // object state
    private FieldDef fieldDef;
    private RowData rowData;

    @Override
    public PValueSource copySource()
    {
        throw new UnsupportedOperationException("not supported yet");
    }
}
