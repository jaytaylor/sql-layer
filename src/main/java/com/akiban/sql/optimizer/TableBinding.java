
package com.akiban.sql.optimizer;

import com.akiban.ais.model.Columnar;

/**
 * A table binding: stored in the UserData of a FromBaseTable and
 * referring to a Table in the AIS.
 */
public class TableBinding 
{
    private Columnar table;
    private boolean nullable;
        
    public TableBinding(Columnar table, boolean nullable) {
        this.table = table;
        this.nullable = nullable;
    }

    public TableBinding(TableBinding other) {
        this.table = other.table;
    }

    public Columnar getTable() {
        return table;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String toString() {
        return table.toString();
    }
}
