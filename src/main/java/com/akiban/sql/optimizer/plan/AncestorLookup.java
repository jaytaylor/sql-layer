/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

public class AncestorLookup extends BaseAccessPath
{
    private TableSource descendant;

    public AncestorLookup(TableSource descendant) {
        this.descendant = descendant;
    }

    public TableSource getDescendant() {
        return descendant;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        descendant = (TableSource)descendant.duplicate();
    }

    @Override
    public String toString() {
        return super.toString() + "(" + descendant + ")";
    }

}
