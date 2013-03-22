
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.UserTable;

import java.util.HashMap;
import java.util.Map;

public abstract class TableTreeBase<T extends TableSubTreeBase.TableNodeBase<T>>
                        extends TableSubTreeBase<T>
{
    private Map<UserTable,T> map = new HashMap<>();

    public TableTreeBase() {
        super(null);
    }

    public T getNode(UserTable table) {
        return map.get(table);
    }

    protected abstract T createNode(UserTable table) ;

    public T addNode(UserTable table) {
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
