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

import java.util.Map;
import java.util.HashMap;

public class DuplicateMap
{
    private Map<Duplicatable,Duplicatable> map;
    private Map<TableTree,TableTree> trees;

    public DuplicateMap() {
        map = new HashMap<Duplicatable,Duplicatable>();
        trees = new HashMap<TableTree,TableTree>();
    }

    public <T extends Duplicatable> T get(T duplicatable) {
        return (T)map.get(duplicatable);
    }

    public <T extends Duplicatable> void put(T orig, T copy) {
        map.put(orig, copy);
    }
    
    public TableNode duplicate(TableNode t) {
        TableTree otree = t.getTree();
        TableTree ntree = trees.get(otree);
        if (ntree == null) {
            ntree = new TableTree();
            trees.put(otree, ntree);
        }
        return ntree.addNode(t.getTable());
    }

}
