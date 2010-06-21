package com.akiban.cserver.loader;

import java.util.List;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

public abstract class GenerateParentTask extends Task {
    public GenerateParentTask(BulkLoader loader, UserTable table) {
        super(loader, table, "$parent");
    }

    public List<Column> pkColumns() {
        return pkColumns;
    }

    protected void pkColumns(List<Column> pkColumns) {
        if (this.pkColumns != null) {
            throw new BulkLoader.InternalError(pkColumns.toString());
        }
        this.pkColumns = pkColumns;
    }

    private List<Column> pkColumns;
}