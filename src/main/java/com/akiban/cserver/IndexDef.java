package com.akiban.cserver;

import com.akiban.ais.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Defines an Index within the Chunk Server
 * 
 * 
 * @author peter
 * 
 */
public class IndexDef {

    private final Index index;

    private final String treeName;

    // Identifies fields within the row that form the key part of the index entry.
    private final int[] fields;

    private final RowDef rowDef;

    private boolean hkeyEquivalent;

    // indexKeyFields[i].field is, if set (>-1), is the position within the row of the ith index field.
    // These values must match the contents of fields. If indexKeyFields[i].field is not set, then
    // hKeyLoc specifies where within the hkey the ith index field comes from.
    private H2I[] indexKeyFields;

    // Specifies the layout of an hkey, as derived from an index entry. If hKeyFields[i].ordinal is
    // set (>-1), then the ith field of the hkey is that ordinal. Otherwise, hKeyFields[i].indexLoc
    // specifies the position within the index entry of the ith hkey field.
    private I2H[] hKeyFields;

    /*
     * Structure that determines how a field in a table binds to a key segment of an index key. An H2I defines
     * the field's position in an hkey and/or an h-row. hKeyLoc is used only as a last resort.
     * (This is important for covering index analysis.)
     */
    public static class H2I
    {
        public String toString()
        {
            return
                fieldIndex == -1
                ? String.format("H2I<hKeyLoc=%s>", hKeyLoc)
                : String.format("H2I<fieldIndex=%s>", fieldIndex);
        }

        public int fieldIndex()
        {
            return fieldIndex;
        }

        public int hKeyLoc()
        {
            return hKeyLoc;
        }

        static H2I fromField(int fieldIndex)
        {
            return new H2I(fieldIndex, -1);
        }

        private H2I(int fieldIndex, int hKeyLoc)
        {
            this.fieldIndex = fieldIndex;
            this.hKeyLoc = hKeyLoc;
        }

        static H2I fromHKeyField(int hKeyLoc)
        {
            return new H2I(-1, hKeyLoc);
        }

        private final int fieldIndex;
        private final int hKeyLoc;
    }

    /*
     * Structure that binds information about an index key segment to an hkey
     * field. If rowDef is set, it's used to set an hkey ordinal. Otherwise,
     * indexKeyLoc is set, and it stores the position within the index of the hkey
     * field.
     */
    public static class I2H
    {
        public String toString()
        {
            return
                isOrdinalType()
                ? String.format("I2H<ordinal=%s>", rowDef.getOrdinal())
                : String.format("I2H<fieldIndex=%s, indexKeyLoc=%s>", fieldIndex, indexKeyLoc);
        }

        public int fieldIndex()
        {
            return fieldIndex;
        }

        public int indexKeyLoc()
        {
            return indexKeyLoc;
        }

        public int ordinal()
        {
            return rowDef.getOrdinal();
        }

        public boolean isOrdinalType()
        {
            return rowDef != null;
        }

        I2H(final RowDef rowDef)
        {
            this.rowDef = rowDef;
            this.indexKeyLoc = -1;
            this.fieldIndex = -1;
        }

        I2H(int indexKeyLoc, int fieldIndex)
        {
            this.rowDef = null;
            this.indexKeyLoc = indexKeyLoc;
            this.fieldIndex = fieldIndex;
        }

        private final RowDef rowDef;
        private final int fieldIndex;
        private final int indexKeyLoc;
    }

    public IndexDef(String treeName, RowDef rowDef, Index index)
    {
        this.index = index;
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

    public String getName() {
        return index.getIndexName().getName();
    }

    public String getTreeName() {
        return treeName;
    }

    public int getId() {
        return index.getIndexId();
    }

    public int[] getFields() {
        return fields;
    }

    public boolean isPkIndex() {
        return rowDef.isGroupTable() ? false : index.isPrimaryKey();
    }

    /**
     * True if this index represents fields matching the pkFields of the root
     * table. If so, then there is no separately stored index tree.
     */
    public boolean isHKeyEquivalent() {
        return hkeyEquivalent;
    }

    public boolean isUnique() {
        return index.isUnique();
    }

    public RowDef getRowDef() {
        return rowDef;
    }

    public int getIndexKeySegmentCount() {
        return fields.length;
    }

    public H2I[] indexKeyFields()
    {
        return indexKeyFields;
    }

    public I2H[] hkeyFields()
    {
        return hKeyFields;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rowDef.getTableName());
        sb.append(":");
        sb.append(getName());
        sb.append("(");
        for (int i = 0; i < fields.length; i++) {
            sb.append(i == 0 ? "" : ",");
            sb.append(rowDef.getFieldDef(fields[i]).getName());
        }
        sb.append(")->");
        sb.append(treeName);
        if (hkeyEquivalent) {
            sb.append("=hkey");
        }
        return sb.toString();
    }


    // TODO: 1) hkey equivalence needs to account for collation, character sets and other
    // TODO:    elements that affect ordering.
    // TODO: 2) This won't work from group table indexes whose columns span multiple user tables.
    void computeFieldAssociations(RowDefCache rowDefCache, List<RowDef> path)
    {
        computeHKeyEquivalence(path);
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
                if (column == null) {
                    // hkey column is a pk-less table counter
                    h2i = H2I.fromHKeyField(hKeyColumn.positionInHKey());
                    h2iList.add(h2i);
                    indexColumns.add(null);
                } else {
                    // hkey column is a real column
                    if (!indexColumns.contains(column)) {
                        if (index.getTable().getColumns().contains(hKeyColumn.column())) {
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
        }
        indexKeyFields = h2iList.toArray(new H2I[h2iList.size()]);
        // hKeyFields is a list of I2H objects used to construct hkey values from index entries.
        // There are two types of I2H entries, "ordinal" entries, and entries that identify index fields.
        // An ordinal entry, identifying a user table, appears in the hkey precedes all the hkey values
        // from that user table. Non-ordinal I2H objects also contain the position of the hkey column
        // in the table, for ues in index analysis (PersistitStoreIndexManager.analyzeIndex).
        List<I2H> i2hList = new ArrayList<I2H>();
        for (HKeySegment hKeySegment : hKey.segments()) {
            i2hList.add(new I2H(rowDefCache.rowDef(hKeySegment.table())));
            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column column = hKeyColumn.column();
                // hKeyColumn.column() will be null for an hkey segment from a pk-less table.
                // For such a column, a null has already been placed in indexColumns, so this use
                // of indexColumns.indexOf() should just work. (This would not work if an index could
                // contain multiple pk-less table counters.) In this case, hKeyColumnFieldPosition,
                // (used to set I2H.fieldPosition) should not be used.
                int hKeyColumnIndexPosition = indexColumns.indexOf(column);
                int hKeyColumnFieldPosition = column == null ? -1 : column.getPosition();
                i2hList.add(new I2H(hKeyColumnIndexPosition, hKeyColumnFieldPosition));
            }
        }
        hKeyFields = i2hList.toArray(new I2H[i2hList.size()]);
    }

    private void computeHKeyEquivalence(List<RowDef> path)
    {
        hkeyEquivalent = true;
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
    }

    @Override
    public boolean equals(final Object o) {
        final IndexDef def = (IndexDef) o;
        return
            getName().equals(def.getName()) &&
            treeName.equals(def.treeName) &&
            getId() == def.getId() &&
            Arrays.equals(fields, def.fields) &&
            isPkIndex() == def.isPkIndex() &&
            isUnique() == def.isUnique();
    }

    @Override
    public int hashCode()
    {
        return getName().hashCode() ^ treeName.hashCode() ^ getId() ^ Arrays.hashCode(fields);
    }

}
