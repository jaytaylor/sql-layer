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

package com.akiban.server;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.server.service.tree.TreeCache;
import com.akiban.server.service.tree.TreeLink;

public class IndexDef implements TreeLink {
    private final TableIndex index;
    private final String treeName;
    // Identifies fields within the row that form the key part of the index entry.
    private final int[] fields;
    private final RowDef rowDef;
    private AtomicReference<TreeCache> treeCache = new AtomicReference<TreeCache>();


    public IndexDef(String treeName, RowDef rowDef, TableIndex index)
    {
        this.index = index;
        index.indexDef(this);
        this.treeName = treeName;
        this.rowDef = rowDef;
        this.fields = new int[index.getColumns().size()];
        for (IndexColumn indexColumn : index.getColumns()) {
            int positionInRow = indexColumn.getColumn().getPosition();
            int positionInIndex = indexColumn.getPosition();
            this.fields[positionInIndex] = positionInRow;
        }
    }

    public Index index()
    {
        return index;
    }

    public String getTreeName() {
        return treeName;
    }

    public int[] getFields() {
        return fields;
    }

    public RowDef getRowDef() {
        return rowDef;
    }

    public int getIndexKeySegmentCount() {
        return fields.length;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rowDef.getTableName());
        sb.append(":");
        sb.append(index.getIndexName().getName());
        sb.append("(");
        for (int i = 0; i < fields.length; i++) {
            sb.append(i == 0 ? "" : ",");
            sb.append(rowDef.getFieldDef(fields[i]).getName());
        }
        sb.append(")->");
        sb.append(treeName);
        if (index.isHKeyEquivalent()) {
            sb.append("=hkey");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        final IndexDef def = (IndexDef) o;
        return index.equals(def.index) &&
            treeName.equals(def.treeName) &&
            Arrays.equals(fields, def.fields);
    }

    @Override
    public int hashCode()
    {
        return index.getIndexName().getName().hashCode() ^ treeName.hashCode() ^
               index.getIndexId() ^ Arrays.hashCode(fields);
    }

    @Override
    public String getSchemaName() {
        return rowDef.getSchemaName();
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
