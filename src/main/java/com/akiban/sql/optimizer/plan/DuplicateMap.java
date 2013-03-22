
package com.akiban.sql.optimizer.plan;

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
