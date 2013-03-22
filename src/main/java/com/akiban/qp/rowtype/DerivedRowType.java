
package com.akiban.qp.rowtype;

public abstract class DerivedRowType extends RowType
{
    @Override
    public final DerivedTypesSchema schema()
    {
        return schema;
    }

    // For use by subclasses

    protected DerivedRowType(DerivedTypesSchema schema, int typeId)
    {
        super(typeId);
        this.schema = schema;
    }

    private final DerivedTypesSchema schema;
}
