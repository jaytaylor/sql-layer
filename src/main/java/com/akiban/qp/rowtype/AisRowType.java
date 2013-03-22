
package com.akiban.qp.rowtype;

public abstract class AisRowType extends RowType {

    @Override
    public Schema schema() {
        return schema;
    }

    public AisRowType(Schema schema, int typeId) {
        super(typeId);
        this.schema = schema;
    }

    private final Schema schema;
}
