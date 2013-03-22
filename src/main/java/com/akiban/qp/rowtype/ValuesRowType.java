
package com.akiban.qp.rowtype;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSources;

import java.util.Arrays;

public class ValuesRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return "values(" + Arrays.toString(types == null ? tInstances : types) + ')';
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return (types == null) ? ((tInstances == null) ? 0 : tInstances.length) : types.length;
    }

    @Override
    public AkType typeAt(int index) {
        return types[index];
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        // Hopefully we were created in a types3 way and therefore have tInstances. If not, we have no idea what
        // our nullability is, so we have to be pessimistic and assume everything is nullable.
        return tInstances != null ? tInstances[index] : PValueSources.fromAkType(types[index], true);
    }

    // ValuesRowType interface

    public ValuesRowType(DerivedTypesSchema schema, int typeId, AkType... types)
    {
        super(schema, typeId);
        this.types = types;
        this.tInstances = null;
    }

    public ValuesRowType(DerivedTypesSchema schema, int typeId, TInstance... fields) {
        super(schema, typeId);
        this.types = null;
        this.tInstances = fields;
    }

    // Object state

    private final AkType[] types;
    private TInstance[] tInstances;
}
