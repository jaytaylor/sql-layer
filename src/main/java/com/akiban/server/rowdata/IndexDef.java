/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
    private AtomicReference<TreeCache> treeCache = new AtomicReference<TreeCache>();


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
