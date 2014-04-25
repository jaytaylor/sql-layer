/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.ais.util;

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;

import java.util.UUID;

public class UuidAssigner extends AbstractVisitor
{
    @Override
    public void visit(Table table) {
        if (table.getUuid() == null) {
            assignedAny = true;
            table.setUuid(UUID.randomUUID());
        }
    }

    @Override
    public void visit(Column column) {
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
