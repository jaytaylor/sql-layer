
package com.akiban.server.rowdata;

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
}
