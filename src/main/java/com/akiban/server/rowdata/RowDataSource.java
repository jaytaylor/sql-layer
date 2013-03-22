
package com.akiban.server.rowdata;

public interface RowDataSource {
    void bind(FieldDef fieldDef, RowData rowData);
}
