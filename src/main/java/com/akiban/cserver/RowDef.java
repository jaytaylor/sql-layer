package com.akiban.cserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Contain the relevant schema information for one version of a table
 * definition. Instances of this class acquire table definition data from the
 * AIS, interpret it, and provide information on how to encode and decode fields
 * from a RowData structure.
 * 
 * 
 * @author peter
 * 
 */
public class RowDef {
    /**
     * Array of FieldDef, one per column
     */
    private final FieldDef[] fieldDefs;

    /**
     * Unique, permanent handle for a version of a table definition
     */
    private final int rowDefId;

    /**
     * Denotes position within a group; this value is used to identify a table
     * within the group structure in the hkey.
     */
    private int ordinal;

    /**
     * Field(s) that constitute the primary key for this table. Must not be
     * empty; will usually have one element. Multiple elements define a compound
     * primary key.
     */
    private int[] pkFields;

    /**
     * Parent table. This denotes the table hierarchy within a group. Value is
     * zero, meaning there is no parent RowDef, if this table is a root table.
     */
    private int parentRowDefId;

    /**
     * Field(s) that constitute the foreign key by which this row is joined to
     * its parent table.
     */
    private int[] parentJoinFields;

    /**
     * Schema's name for this table.
     */
    private String tableName;
    /**
     * Name of the tree storing this table - same as the group table name
     */
    private String treeName;

    /**
     * Schema this RowDef belongs to.
     */
    private String schemaName;

    /**
     * Field index of the auto-increment column; -1 if none.
     */
    private int autoIncrementField = -1;

    /**
     * RowDefs of constituent user tables. Populated only if this is the RowDef
     * for a group table. Null if this is the RowDef for a user table.
     */
    private RowDef[] userTableRowDefs;

    /**
     * If this is a user table, the rowDefId of the group table it belongs too,
     * else 0.
     */
    private int groupRowDefId;

    /**
     * For a user table, the column position of the first column of the user
     * table relative to the group table.
     */
    private int columnOffset;

    /**
     * For a user table, the number of Persistit Key segments uses to encode the
     * hkey for rows of this table.
     */
    private int hkeyDepth;
    /**
     * Array of index definitions for this row
     */
    private IndexDef[] indexDefs;

    /**
     * Array computed by the {@link #preComputeFieldCoordinates(FieldDef[])}
     * method to assist in looking up a field's offset and length.
     */
    private final int[][] fieldCoordinates;

    /**
     * Row type
     */
    private RowType rowType;

    /**
     * Array computed by the {@link #preComputeFieldCoordinates(FieldDef[])}
     * method to assist in looking up a field's offset and length.
     */
    private final byte[][] varLenFieldMap;
    /**
     * Factory method used for convenience in tests
     * 
     * @param rowDefId
     * @param fieldDefs
     * @param tableName
     * @param treeName
     * @param pkFields
     * @return
     */
    public static RowDef createRowDef(final int rowDefId,
            final FieldDef[] fieldDefs, final String tableName,
            String treeName, final int[] pkFields) {
        final RowDef rowDef = new RowDef(rowDefId, fieldDefs);
        rowDef.setTableName(tableName);
        rowDef.setTreeName(treeName);
        rowDef.setPkFields(pkFields);
        rowDef.setParentRowDefId(0);
        rowDef.setParentJoinFields(new int[0]);
        rowDef.setIndexDefs(new IndexDef[0]);
        return rowDef;
    }

    /**
     * Factory method used for convenience in tests
     * 
     * @param rowDefId
     * @param fieldDefs
     * @param tableName
     * @param groupTableName
     * @param pkFields
     * @param parentRowDefId
     * @param parentJoinFields
     * @return
     */
    public static RowDef createRowDef(final int rowDefId,
            final FieldDef[] fieldDefs, final String tableName,
            final String groupTableName, final int[] pkFields,
            final int parentRowDefId, final int[] parentJoinFields) {
        final RowDef rowDef = new RowDef(rowDefId, fieldDefs);
        rowDef.setTableName(tableName);
        rowDef.setTreeName(groupTableName);
        rowDef.setPkFields(pkFields);
        rowDef.setParentRowDefId(parentRowDefId);
        rowDef.setParentJoinFields(parentJoinFields);
        rowDef.setIndexDefs(new IndexDef[0]);
        return rowDef;
    }

    public RowDef(final int rowDefId, final FieldDef[] fieldDefs) {
        this.rowDefId = rowDefId;
        this.fieldDefs = fieldDefs;
        for (int index = 0; index < fieldDefs.length; index++) {
            final FieldDef fieldDef = fieldDefs[index];
            fieldDef.setRowDef(this);
            fieldDef.setFieldIndex(index);
        }
        fieldCoordinates = new int[(fieldDefs.length + 7) / 8][];
        varLenFieldMap = new byte[(fieldDefs.length + 7) / 8][];
        preComputeFieldCoordinates(fieldDefs);
    }

    /**
     * Display the fieldCoordinates array
     */
    public String debugToString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < fieldDefs.length; i++) {
            if (i != 0)
                sb.append(",");
            sb.append(fieldDefs[i]);
        }
        sb.append("]\n");
        for (int i = 0; i < fieldCoordinates.length; i++) {
            sb.append("--- " + i + " ---\n");
            int count = 256;
            int remainingBits = fieldDefs.length - (i * 8);
            if (remainingBits >= 0 && remainingBits < 8) {
                count = 1 << remainingBits;
            }
            for (int j = 0; j < count; j += 16) {
                for (int k = 0; k < 16; k++) {
                    sb.append((k % 8) == 0 ? "   " : " ");
                    CServerUtil.hex(sb, fieldCoordinates[i][j + k], 8);
                }
                sb.append("\n");
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    /**
     * An implementation useful while debugging. TODO - replace with something
     * less verbose.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(String.format(
                "RowDef #%d %s (%s.%s) %s ", rowDefId, tableName, treeName,
                getPkTreeName(), rowType));
        if (userTableRowDefs != null) {
            for (int i = 0; i < userTableRowDefs.length; i++) {
                sb.append(i == 0 ? "{" : ",");
                sb.append(userTableRowDefs[i].getRowDefId());
                sb.append("->");
                sb.append(userTableRowDefs[i].getColumnOffset());
                sb.append(":" + userTableRowDefs[i].getFieldCount());
            }
            sb.append("}");
        }
        sb.append("groupColumnOffset, fieldcount " + getColumnOffset() + ", "
                + getFieldCount());

        for (int i = 0; i < fieldDefs.length; i++) {
            sb.append(i == 0 ? "[" : ",");
            sb.append(fieldDefs[i].getType());
            if (pkFields != null) {
                for (int j = 0; j < pkFields.length; j++) {
                    if (pkFields[j] == i) {
                        sb.append("*");
                        break;
                    }
                }
            }
            if (parentJoinFields != null) {
                for (int j = 0; j < parentJoinFields.length; j++) {
                    if (parentJoinFields[j] == i) {
                        sb.append("^");
                        sb.append(j);
                        break;
                    }
                }
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns the offset relative to the start of the byte array represented by
     * the supplied {@link RowData} of the field specified by the supplied
     * fieldIndex, or zero if the field is null. The location is a long which
     * encodes an offset to the data and its length in bytes. If
     * <code>offset</code> and <code>length</code> are the offset and length of
     * the data, respectively, then the location is returned as
     * 
     * <code>(long)offset+((long)length << 32)</code>
     * 
     * For fixed-length fields like numbers, the length is the fixed length of
     * the field, e.g., 8 for BIGINT values. For variable- length fields the
     * length is the number of bytes used in representing the value.
     * 
     * @param rowData
     * @param fieldIndex
     * @return
     */

    public long fieldLocation(final RowData rowData, final int fieldIndex) {
        final int fieldCount = fieldDefs.length;
        if (fieldIndex < 0 || fieldIndex >= fieldCount) {
            throw new IllegalArgumentException("Field index out of bounds: "
                    + fieldIndex);
        }
        int dataStart = rowData.getRowStartData();
        //
        // If NullMap bit is set, return zero immediately
        //
        if (((rowData.getColumnMapByte(fieldIndex / 8) >>> (fieldIndex % 8)) & 1) == 1) {
            return 0;
        }

        if (fieldDefs[fieldIndex].isFixedSize()) {
            //
            // Look up the offset and width of a fixed-width field
            //
            int offset = dataStart;
            int width = 0;
            //
            // Loop over NullMap bytes until reaching fieldIndex
            //
            for (int k = 0; k <= fieldIndex; k += 8) {
                int byteIndex = k >>> 3;
                int mapByte = (~rowData.getColumnMapByte(byteIndex)) & 0xFF;
                //
                // Look up offset and width in the statically-generated
                // fieldCoordinates array.
                //
                int bitCount = fieldIndex - k;
                if (bitCount < 8) {
                    mapByte &= (0xFF >>> (7 - bitCount));
                }
                int fc = fieldCoordinates[byteIndex][mapByte];
                //
                // Decode the offset and width fields
                //
                width = fc >>> 24;
                offset += (fc & 0xFFFFFF) + width;
            }
            //
            // Encode the width and offset fields
            //
            return ((offset & 0xFFFFFF) - width) | (((long) width) << 32);
        } else {
            //
            // Look up the offset and width of a variable-width field.
            // Previous and current refer to the coordinates of the
            // fixed-length fields delimiting the variable-length
            // data.
            //
            int offset = dataStart;
            int width = 0;
            int previous = 0;
            int current = 0;

            for (int k = 0; k < fieldCount; k += 8) {
                if (k <= fieldIndex) {
                    current = current + width;
                }
                int byteIndex = k >>> 3;
                int mapByte = (~rowData.getColumnMapByte(byteIndex)) & 0xFF;
                int bitCount1 = fieldIndex - k;
                //
                // Look up offset and width in the statically-generated
                // fieldCoordinates array.
                //
                if (k <= fieldIndex) {
                    int mbb1 = mapByte;
                    int mask = 0xFF;
                    if (bitCount1 < 7) {
                        mask >>>= (7 - bitCount1);
                        mbb1 &= mask;
                    }
                    int fc1 = fieldCoordinates[byteIndex][mbb1];
                    current = fc1 + (current & 0xFFFFFF);
                    int mbb2 = varLenFieldMap[byteIndex][bitCount1 < 8 ? mbb1
                            : mbb1 + 256] & 0xFF;
                    if (mbb2 != 0) {
                        int fc2 = fieldCoordinates[byteIndex][mbb2];
                        previous = current + fc2 - fc1;
                    }
                }
                //
                // In addition, because the overall size of the fixed-length
                // field array is not encoded, we need to compute the offset
                // of the byte after the last field. That's where the variable-
                // length bytes begin.
                //
                int bitCount2 = fieldCount - k;
                int mbb2 = mapByte;
                if (bitCount2 > 0 && bitCount2 < 8) {
                    mbb2 &= (0xFF >>> (8 - bitCount2));
                }
                int fc = fieldCoordinates[byteIndex][mbb2];
                //
                // Decode the offset and width of the last field
                //
                width = (fc) >>> 24;
                offset += (fc & 0xFFFFFF) + width;
            }
            //
            // Compute the starting and ending offsets (from the beginning of
            // the rowData byte array) of the variable-length segment.
            //
            int start = (int) rowData.getIntegerValue((previous & 0xFFFFFF)
                    + dataStart, previous >>> 24);

            int end = (int) rowData.getIntegerValue((current & 0xFFFFFF)
                    + dataStart, current >>> 24);
            //
            // Encode and return the offset and length
            //
            return (start + offset) | ((long) (end - start) << 32);
        }
    }

    public RowType getRowType() {
        return rowType;
    }

    public int getFieldCount() {
        return fieldDefs.length;
    }

    public FieldDef getFieldDef(final int index) {
        return fieldDefs[index];
    }

    public FieldDef[] getFieldDefs() {
        return fieldDefs;
    }

    public int getGroupRowDefId() {
        return groupRowDefId;
    }

    public IndexDef[] getIndexDefs() {
        return indexDefs;
    }

    public IndexDef getIndexDef(final int indexId) {
        // TODO: Could use a HashMap instead if this linear search proves to be
        // a CPU problem..
        for (int index = 0; index < indexDefs.length; index++) {
            if (indexDefs[index].getId() == indexId) {
                return indexDefs[index];
            }
        }
        return null;
    }

    public int[] getParentJoinFields() {
        return parentJoinFields;
    }

    public int getParentRowDefId() {
        return parentRowDefId;
    }

    public int[] getPkFields() {
        return pkFields;
    }

    public String getPkTreeName() {
        final IndexDef pkIndexDef = getPKIndexDef();
        return pkIndexDef != null ? pkIndexDef.getTreeName() : null;
    }

    public int getRowDefId() {
        return rowDefId;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTreeName() {
        return treeName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean isGroupTable() {
        return userTableRowDefs != null;
    }

    public void setRowType(final RowType rowType) {
        this.rowType = rowType;
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    public void setGroupRowDefId(final int groupRowDefId) {
        this.groupRowDefId = groupRowDefId;
    }

    public void setIndexDefs(IndexDef[] indexDefs) {
        this.indexDefs = indexDefs;
    }

    public void setParentJoinFields(int[] parentJoinFields) {
        this.parentJoinFields = parentJoinFields;
    }

    public void setParentRowDefId(int parentRowDefId) {
        this.parentRowDefId = parentRowDefId;
    }

    public void setPkFields(int[] pkFields) {
        this.pkFields = pkFields;
    }

    public void setAutoIncrementField(int autoIncrementField) {
        this.autoIncrementField = autoIncrementField;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTreeName(final String treeName) {
        this.treeName = treeName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    public int getAutoIncrementField() {
        return autoIncrementField;
    }

    public int getHKeyDepth() {
        return hkeyDepth;
    }

    public int getColumnOffset() {
        return columnOffset;
    }

    public void setColumnOffset(final int columnOffset) {
        this.columnOffset = columnOffset;
    }

    public IndexDef getPKIndexDef() {
        if (!isGroupTable() && indexDefs != null && indexDefs.length > 0) {
            return indexDefs[0];
        } else {
            return null;
        }
    }

    public RowDef[] getUserTableRowDefs() {
        return userTableRowDefs;
    }

    public void setUserTableRowDefs(final RowDef[] userTableRowDefs) {
        this.userTableRowDefs = userTableRowDefs;
    }

    /**
     * Compute lookup tables used to in the {@link #fieldLocation(RowData, int)}
     * method. This method is invoked once when a RowDef is first constructed.
     * 
     * @param fieldDefs
     */
    void preComputeFieldCoordinates(final FieldDef[] fieldDefs) {
        final int fieldCount = fieldDefs.length;
        int voffset = 0;
        for (int field = 0; field < fieldCount; field++) {
            final FieldDef fieldDef = fieldDefs[field];
            final int byteIndex = field / 8;
            final int bitIndex = field % 8;
            final int bit = 1 << bitIndex;
            if (bitIndex == 0) {
                fieldCoordinates[byteIndex] = new int[256];
                varLenFieldMap[byteIndex] = new byte[512];
            }
            final int width;
            if (fieldDef.isFixedSize()) {
                width = fieldDef.getMaxStorageSize();
            } else {
                voffset += fieldDef.getMaxStorageSize();
                width = CServerUtil.varWidth(voffset);
            }
            for (int i = 0; i < bit; i++) {
                int from = fieldCoordinates[byteIndex][i];
                int to = ((from & 0xFFFFFF) + (from >>> 24)) | (width << 24);
                int k = i + bit;
                fieldCoordinates[byteIndex][k] = to;
                for (int j = bitIndex; --j >= 0;) {
                    if ((k & (1 << j)) != 0
                            && !fieldDefs[byteIndex * 8 + j].isFixedSize()) {
                        varLenFieldMap[byteIndex][k] = (byte) (k & ((0xFF >>> (7 - j))));
                        break;
                    }
                }
                for (int j = bitIndex + 1; --j >= 0;) {
                    if ((k & (1 << j)) != 0
                            && !fieldDefs[byteIndex * 8 + j].isFixedSize()) {
                        varLenFieldMap[byteIndex][k + 256] = (byte) (k & ((0xFF >>> (7 - j))));
                        break;
                    }
                }
            }
        }
    }

    void computeRowDefType(final RowDefCache rowDefCache) {
        if (userTableRowDefs != null) {
            rowType = RowType.GROUP;
        } else if (parentRowDefId == 0) {
            rowType = RowType.ROOT;
        } else {
            RowDef parentRowDef = rowDefCache.getRowDef(parentRowDefId);
            assert parentRowDef != null;
            if (parentRowDef.getParentRowDefId() == 0) {
                rowType = RowType.CHILD;
            } else {
                rowType = RowType.GRANDCHILD;
            }
        }
    }

    void computeFieldAssociations(final RowDefCache rowDefCache) {
        for (final IndexDef indexDef : indexDefs) {
            final List<RowDef> path = new ArrayList<RowDef>();
            RowDef def = leafUserRowDef(indexDef.getFields());
            int hkeyDepth = 0;
            while (def != null) {
                path.add(0, def);
                hkeyDepth += 1 + def.getPkFields().length;
                def = def.getParentRowDefId() == 0 ? null : rowDefCache
                        .getRowDef(def.getParentRowDefId());
            }
            if (!isGroupTable()) {
                this.hkeyDepth = hkeyDepth;
            }
            indexDef.computeFieldAssociations(rowDefCache, this, path);
        }
    }

    // TODO -
    // Total hack. This is because all the mechanism that sets up the
    // IndexDef objects for group tables does nothing to record the
    // IndexDef's association to its user RowDef, but we need that to
    // make the index->hkey transformation in SELECT.
    //
    private RowDef leafUserRowDef(final int[] fields) {
        int field = fields[0]; // 
        if (isGroupTable()) {
            for (final RowDef userRowDef : userTableRowDefs) {
                if (userRowDef.getColumnOffset() <= field
                        && userRowDef.getColumnOffset()
                                + userRowDef.getFieldCount() > field) {
                    return userRowDef;
                }
            }
            throw new IllegalStateException(
                    "Didn't find the IndexDef's originating user RowDef");
        } else {
            return this;
        }
    }

    @Override
    public boolean equals(final Object o) {
        final RowDef def = (RowDef) o;
        return rowDefId == def.rowDefId
                && CServerUtil.equals(tableName, def.tableName)
                && CServerUtil.equals(treeName, def.treeName)
                && CServerUtil.equals(schemaName, def.schemaName)
                && Arrays.deepEquals(fieldDefs, def.fieldDefs)
                && Arrays.deepEquals(indexDefs, def.indexDefs)
                && ordinal == def.ordinal
                && Arrays.equals(pkFields, def.pkFields)
                && Arrays.equals(parentJoinFields, def.parentJoinFields);

    }

    @Override
    public int hashCode() {
        return rowDefId ^ CServerUtil.hashCode(tableName)
                ^ CServerUtil.hashCode(treeName)
                ^ CServerUtil.hashCode(schemaName) ^ Arrays.hashCode(fieldDefs)
                ^ Arrays.hashCode(indexDefs) ^ Arrays.hashCode(pkFields)
                ^ Arrays.hashCode(parentJoinFields);
    }


}
