
package com.akiban.server.rowdata;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.server.service.tree.TreeCache;
import com.akiban.server.service.tree.TreeLink;

import java.util.concurrent.atomic.AtomicReference;

public class IndexDef implements TreeLink {
    private final Index index;
    // Identifies fields within the row that form the key part of the index entry.
    private final int[] fields;
    private final RowDef rowDef;
    private AtomicReference<TreeCache> treeCache = new AtomicReference<>();


    public IndexDef(RowDef rowDef, Index index)
    {
        this.index = index;
        index.indexDef(this);
        this.rowDef = rowDef;
        this.fields = new int[index.getKeyColumns().size()];
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            int positionInRow = indexColumn.getColumn().getPosition();
            int positionInIndex = indexColumn.getPosition();
            this.fields[positionInIndex] = positionInRow;
        }
    }

    /**
     * @deprecated Use IndexRowComposition or just IndexColumns where appropriate
     * @return Array of index position to table position
     * */
    public int[] getFields() {
        return fields;
    }

    public RowDef getRowDef() {
        return rowDef;
    }

    public int getIndexKeySegmentCount() {
        return fields.length;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return index.toString() + "[" + getTreeName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("IndexDef deprecated, use Index");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("IndexDef deprecated, use Index");
    }

    // TreeLink interface

    @Override
    public String getSchemaName() {
        return index.getIndexName().getSchemaName();
    }

    @Override
    public String getTreeName() {
        return index.getTreeName();
    }

    @Override
    public void setTreeCache(TreeCache cache) {
       treeCache.set(cache);
    }

    @Override
    public TreeCache getTreeCache() {
        return treeCache.get();
    }
}
