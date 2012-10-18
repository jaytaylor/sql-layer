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

package com.akiban.server.api.dml.scan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataExtractor;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.util.ArgumentValidation;

public class NiceRow extends NewRow {
    private final static int INITIAL_ROW_DATA_SIZE = RowData.CREATE_ROW_INITIAL_SIZE;

    private final Map<Integer,Object> fields;
    private final int tableId;

    public NiceRow(Session session, int tableId, Store store)
    {
        this(tableId, rowDef(session, tableId, store));
    }

    public NiceRow(int tableId, RowDef rowDef)
    {
        super(rowDef);
        ArgumentValidation.notNull("tableId", tableId);
        fields = new TreeMap<Integer, Object>();
        this.tableId = tableId;
    }

    public NiceRow(NewRow copyFrom) {
        this(copyFrom.getTableId(), copyFrom.getRowDef());
        fields.putAll( copyFrom.getFields() );
    }

    @Override
    public Object put(int index, Object object) {
        return fields.put(index, object);
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public Object get(int column) {
        return fields.get(column);
    }

    @Override
    public boolean hasValue(int columnId) {
        return fields.containsKey(columnId);
    }

    @Override
    public Object remove(int columnId) {
        return fields.remove(columnId);
    }

    @Override
    public Map<Integer,Object> getFields() {
        return fields;
    }

    public static NewRow fromRowData(RowData origData, RowDef rowDef)
    {
        Set<Integer> activeColumns = new HashSet<Integer>();
        for(int fieldIndex=0, fieldsCount=rowDef.getFieldCount(); fieldIndex < fieldsCount; ++fieldIndex) {
            final long location = rowDef.fieldLocation(origData, fieldIndex);
            // Null != not specified. NewRow, NiceRow, RowData all need the concept of specified vs not-specified
            // fields.
            if (true) { // location != 0) {
                activeColumns.add( fieldIndex );
            }
        }

        NewRow retval = new NiceRow(rowDef.getRowDefId(), rowDef);
        RowDataExtractor extractor = new RowDataExtractor(origData, rowDef);
        for (int pos : activeColumns) {
            Object value = extractor.get(rowDef.getFieldDef(pos));
            Object old = retval.put(pos, value);
            assert old == null : String.format("put(%s, %s) --> %s", pos, value, old);
        }

        return retval;
    }

    @Override
    public RowData toRowData() {
        final Object[] objects = new Object[ rowDef.getFieldCount() ];
        for (Map.Entry<Integer,Object> entry : fields.entrySet()) {
            objects[ entry.getKey() ] = entry.getValue();
        }
        if (rowDef.hasAkibanPK() && fields.size() == rowDef.getFieldCount() - 1) {
            // Last column will be filled in by the row id counter. Need a non-null value for now.
            objects[fields.size()] = -1;
        }
        final RowData retval = new RowData(new byte[INITIAL_ROW_DATA_SIZE]);
        retval.createRow(rowDef, objects, true);

        return retval;
    }

    @Override
    public ColumnSelector getActiveColumns() {
        return new SetColumnSelector(fields.keySet());
    }

    @Override
    public boolean isColumnNull(int columnId) {
        return get(columnId) == null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append("[{");
        sb.append( rowDef.table().getName().getTableName() );
        if (fields.isEmpty()) {
            return sb.append("} <empty>]").toString();
        }
        sb.append("} ");
        int nextExpectedPos = 0;
        for (Map.Entry<Integer,Object> entry : fields.entrySet()) {
            final int pos = entry.getKey();
            if (pos != nextExpectedPos) {
                sb.append("... ");
            }
            
            final Object value = entry.getValue();
            sb.append('(').append(entry.getKey()).append(": ");
            if (value != null) {
                sb.append(value.getClass().getSimpleName()).append(' ');
            }
            sb.append(value).append(") ");

            nextExpectedPos = pos + 1;
        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NiceRow niceRow = (NiceRow) o;

        return fields.equals(niceRow.fields) && (tableId == niceRow.tableId);

    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + tableId;
        return result;
    }
}
