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

import java.util.Map;
import java.util.HashMap;

public class DuplicateMap
{
    private Map<Duplicatable,Duplicatable> map;
    private Map<TableTree,TableTree> trees;

    public DuplicateMap() {
        map = new HashMap<>();
        trees = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
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
