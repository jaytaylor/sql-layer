
package com.akiban.server.api;

import com.akiban.server.error.TableDefinitionMismatchException;
import com.akiban.server.rowdata.RowData;

public final class LegacyUtils {
    public static Integer matchRowDatas(RowData one, RowData two) {
        if (one == null) {
            return (two == null) ? null : two.getRowDefId();
        }
        if (two == null) {
            return one.getRowDefId();
        }
        if (one.getRowDefId() == two.getRowDefId()) {
            return one.getRowDefId();
        }
        throw new TableDefinitionMismatchException (one.getRowDefId(), two.getRowDefId());
    }
}
