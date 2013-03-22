
package com.akiban.ais.util;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.UserTable;

import java.util.UUID;

public class UuidAssigner extends NopVisitor {

    @Override
    public void visitUserTable(UserTable userTable) {
        if (userTable.getUuid() == null) {
            assignedAny = true;
            userTable.setUuid(UUID.randomUUID());
        }
    }

    @Override
    public void visitColumn(Column column) {
        if (column.getUuid() == null) {
            assignedAny = true;
            column.setUuid(UUID.randomUUID());
        }
    }

    public boolean assignedAny() {
        return assignedAny;
    }

    private boolean assignedAny = false;
}
