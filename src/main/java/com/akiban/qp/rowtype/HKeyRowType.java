
package com.akiban.qp.rowtype;

import com.akiban.ais.model.HKey;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

public class HKeyRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return "HKey";
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return nFields;
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        return hKey().column(index).tInstance();
    }

    @Override
    public AkType typeAt(int index)
    {
        return hKey().columnType(index);
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    // HKeyRowType interface
    
    public HKeyRowType(DerivedTypesSchema schema, HKey hKey)
    {
        super(schema, schema.nextTypeId());
        this.hKey = hKey;
        this.nFields = hKey.nColumns();
    }

    // Object state

    private final int nFields;
    private HKey hKey;
}
