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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.Table;
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

    private static class H2I
    {
        public static H2I fromField(int fieldIndex) {
            return new H2I(0, fieldIndex, -1);
        }

        public static H2I fromHKeyField(int hKeyLoc) {
            return new H2I(-1, -1, hKeyLoc);
        }

        private H2I(int depth, int fieldIndex, int hKeyLoc) {
            this.depth = depth;
            this.fieldIndex = fieldIndex;
            this.hKeyLoc = hKeyLoc;
        }

        public final int depth;
        public final int fieldIndex;
        public final int hKeyLoc;
    }

    private static class I2H
    {
        public static I2H fromOrdinal(int ordinal) {
            return new I2H(ordinal, -1, -1);
        }

        public static I2H fromIndexRow(int indexRowPos, int fieldPos) {
            return new I2H(-1, indexRowPos, fieldPos);
        }

        private I2H(int ordinal, int indexRowPos, int fieldPos) {
            this.ordinal = ordinal;
            this.indexRowPos = indexRowPos;
            this.fieldPos = fieldPos;
        }

        public final int ordinal;
        public final int indexRowPos;
        public final int fieldPos;
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

    private static IndexRowComposition makeIndexRowComposition(List<H2I> h2iLst) {
        int[] depths = new int[h2iLst.size()];
        int[] fieldPositions = new int[h2iLst.size()];
        int[] hkeyPositions = new int[h2iLst.size()];
        int i = 0;
        for(H2I h2i : h2iLst) {
            depths[i] = h2i.depth;
            fieldPositions[i] = h2i.fieldIndex;
            hkeyPositions[i] = h2i.hKeyLoc;
            ++i;
        }
        return new IndexRowComposition(depths, fieldPositions, hkeyPositions);
    }

    private static IndexToHKey makeIndexToHKey(List<I2H> i2hList) {
        int[] ordinals = new int[i2hList.size()];
        int[] indexRowPositions = new int[i2hList.size()];
        int[] fieldPositions = new int[i2hList.size()];
        int i = 0;
        for(I2H i2h : i2hList) {
            ordinals[i] = i2h.ordinal;
            indexRowPositions[i] = i2h.indexRowPos;
            fieldPositions[i] = i2h.fieldPos;
            ++i;
        }
        return new IndexToHKey(ordinals, indexRowPositions, fieldPositions);
    }

    void computeFieldAssociations(Map<Table,Integer> ordinalMap) {
        computeHKeyEquivalence();
        // indexKeyFields is a list of H2I objects which map row and hkey fields to the fields of an index.
        // The leading index fields are exactly the fields identified by IndexDef.fields, i.e., the declared
        // index columns. The remaining index fields are whatever fields are necessary to ensure that the
        // index contains all hkey fields. When an index field could be filled in from the row or the hkey,
        // the preference is to use the value from the row. This simplifies covering index analysis.
        //
        // The complete set of index fields is tracked by the indexColumns variable. This variable serves
        // two purposes. First, after including everything from IndexDef.fields, it is used to figure out if
        // an hkey column is already present. Second, it is used to compute hKeyFields.
        List<H2I> h2iList = new ArrayList<H2I>();
        List<Column> indexColumns = new ArrayList<Column>();
        // Add index key fields
        for (int fieldPosition : fields) {
            h2iList.add(H2I.fromField(fieldPosition));
            Column column = rowDef.getFieldDefs()[fieldPosition].column();
            indexColumns.add(column);
        }
        
        // Add hkey fields not already included
        HKey hKey = index.hKey();
        for (HKeySegment hKeySegment : hKey.segments()) {
            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column column = hKeyColumn.column();
                H2I h2i;
                if (!indexColumns.contains(column)) {
                    if (index.getTable().getColumnsIncludingInternal().contains(hKeyColumn.column())) {
                        h2i = H2I.fromField(hKeyColumn.column().getPosition());
                    } else {
                        assert rowDef.isUserTable();
                        h2i = H2I.fromHKeyField(hKeyColumn.positionInHKey());
                    }
                    h2iList.add(h2i);
                    indexColumns.add(hKeyColumn.column());
                }
            }
        }
        index.indexRowComposition(makeIndexRowComposition(h2iList));

        // hKeyFields is a list of I2H objects used to construct hkey values from index entries.
        // There are two types of I2H entries, "ordinal" entries, and entries that identify index fields.
        // An ordinal entry, identifying a user table, appears in the hkey precedes all the hkey values
        // from that user table. Non-ordinal I2H objects also contain the position of the hkey column
        // in the table, for use in index analysis (PersistitStoreIndexManager.analyzeIndex).
        List<I2H> i2hList = new ArrayList<I2H>();
        for (HKeySegment hKeySegment : hKey.segments()) {
            Integer ordinal = ordinalMap.get(hKeySegment.table());
            assert ordinal != null : hKeySegment.table();
            i2hList.add(I2H.fromOrdinal(ordinal));
            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column column = hKeyColumn.column();
                // hKeyColumn.column() will be null for an hkey segment from a pk-less table.
                // For such a column, a null has already been placed in indexColumns, so this use
                // of indexColumns.indexOf() should just work. (This would not work if an index could
                // contain multiple pk-less table counters.) In this case, hKeyColumnFieldPosition,
                // (used to set I2H.fieldPosition) should not be used.
                int hKeyColumnIndexPosition = indexColumns.indexOf(column);
                int hKeyColumnFieldPosition = column == null ? -1 : column.getPosition();
                i2hList.add(I2H.fromIndexRow(hKeyColumnIndexPosition, hKeyColumnFieldPosition));
            }
        }
        index.indexToHKey(makeIndexToHKey(i2hList));
    }

    private void computeHKeyEquivalence()
    {
        boolean hkeyEquivalent = false;
        /*
        boolean heyEquivalent = true;
        // Collect the HKeyColumns of the index's hkey
        List<HKeyColumn> hKeyColumns = new ArrayList<HKeyColumn>();
        for (HKeySegment hKeySegment : index.hKey().segments()) {
            hKeyColumns.addAll(hKeySegment.columns());
        }
        // Scan hkey columns and index columns and see if they match
        Iterator<HKeyColumn> hKeyColumnScan = hKeyColumns.iterator();
        Iterator<IndexColumn> indexColumnScan = index.getColumns().iterator();
        while (hkeyEquivalent && hKeyColumnScan.hasNext() && indexColumnScan.hasNext()) {
            Column hKeyColumn = hKeyColumnScan.next().column();
            Column indexColumn = indexColumnScan.next().getColumn();
            hkeyEquivalent = hKeyColumn == indexColumn;
        }
        if (hkeyEquivalent && !hKeyColumnScan.hasNext() && indexColumnScan.hasNext()) {
            hkeyEquivalent = false;
        }
        */
        index.isHKeyEquivalent(hkeyEquivalent);
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
