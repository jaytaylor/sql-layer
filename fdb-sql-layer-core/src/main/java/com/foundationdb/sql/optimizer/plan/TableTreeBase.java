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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.Table;

import java.util.HashMap;
import java.util.Map;

public abstract class TableTreeBase<T extends TableSubTreeBase.TableNodeBase<T>>
                        extends TableSubTreeBase<T>
{
    private Map<Table,T> map = new HashMap<>();

    public TableTreeBase() {
        super(null);
    }

    public T getNode(Table table) {
        return map.get(table);
    }

    protected abstract T createNode(Table table) ;

    public T addNode(Table table) {
        T node = getNode(table);
        if (node != null)
            return node;
        node = createNode(table);
        map.put(table, node);
        if (root == null) {
            root = node;
            return node;
        }
        T toInsert = node;
        if (node.getDepth() < root.getDepth()) {
            toInsert = root;
            root = node;
        }
        Table parentTable = toInsert.getTable().getParentTable();
        assert (parentTable != null);
        T parent = addNode(parentTable);
        assert ((toInsert.getParent() == null) && // Brand new or old root.
                (toInsert.getNextSibling() == null));
        toInsert.setParent(parent);
        toInsert.setNextSibling(parent.getFirstChild());
        parent.setFirstChild(toInsert);
        return node;
    }

}
