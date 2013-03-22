/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
