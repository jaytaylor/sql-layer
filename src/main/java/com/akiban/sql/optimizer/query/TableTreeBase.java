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

package com.akiban.sql.optimizer.query;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class TableTreeBase<T extends TableSubTreeBase.TableNodeBase<T>>
                        extends TableSubTreeBase<T>
{
    private Map<UserTable,T> map = new HashMap<UserTable,T>();

    public TableTreeBase() {
        super(null);
    }

    public T getNode(UserTable table) {
        return map.get(table);
    }

    protected abstract T createNode(UserTable table) throws StandardException;

    public T addNode(UserTable table) throws StandardException {
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
        UserTable parentTable = ((UserTable)toInsert.getTable()).parentTable();
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
