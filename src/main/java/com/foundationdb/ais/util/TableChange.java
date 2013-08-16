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

public class TableChange {
    public static enum ChangeType { ADD, DROP, MODIFY }

    private final String oldName;
    private final String newName;
    private final ChangeType changeType;

    private TableChange(String oldName, String newName, ChangeType changeType) {
        this.oldName = oldName;
        this.newName = newName;
        this.changeType = changeType;
    }

    public String getOldName() {
        return oldName;
    }

    public String getNewName() {
        return newName;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    @Override
    public String toString() {
        if(oldName != null && newName == null)
            return changeType + ":" + oldName;
        if(oldName == null && newName != null)
            return changeType + ":" + newName;
        return changeType + ":" + oldName + "->" + newName;
    }

    public static TableChange createAdd(String name) {
        return new TableChange(null, name, ChangeType.ADD);
    }

    public static TableChange createDrop(String name) {
        return new TableChange(name, null, ChangeType.DROP);
    }

    public static TableChange createModify(String oldName, String newName) {
        return new TableChange(oldName, newName, ChangeType.MODIFY);
    }
}
