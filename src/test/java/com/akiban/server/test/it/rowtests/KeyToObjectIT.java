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

package com.akiban.server.test.it.rowtests;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.UnsupportedIndexDataTypeException;
import com.akiban.server.store.IndexVisitor;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.util.WrappingByteSource;
import com.persistit.Key;
import com.persistit.Value;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KeyToObjectIT extends OldTypeITBase {

    @Before
    public final void turnOnIndexes() {
        setCreateIndexes(true);
    }

    /**
     * Internal helper for comparing all indexed values in an index tree to their values in the row after
     * going through Encoder.toObject(RowData) and Encoder.toObject(Key), respectively.
     * <p><b>Note:</b> For test simplicity, the values in the row must be in index order.</p>
     * @param tableId Table to scan.
     * @param expectedRowCount Rows expected from a full table scan.
     * @param indexName Name of index to compare to.
     * @throws Exception On error.
     */
    private void testKeyToObject(int tableId, int expectedRowCount, String indexName) throws Exception {
        final Table table = getUserTable(tableId);
        final Index index = table.getIndex(indexName);
        assertNotNull("expected index named: "+indexName, index);
        
        final List<NewRow> allRows = scanAll(scanAllRequest(tableId));
        assertEquals("rows scanned", expectedRowCount, allRows.size());

        final Iterator<NewRow> rowIt = allRows.iterator();

        persistitStore().traverse(session(), index, new IndexVisitor() {
            private int rowCounter = 0;

            @Override
            protected void visit(Key key, Value value) {
                if(!rowIt.hasNext()) {
                    Assert.fail("More index entries than rows: rows("+allRows+") index("+index+")");
                }

                final NewRow row = rowIt.next();
                key.indexTo(0);

                PersistitKeyValueSource valueSource = new PersistitKeyValueSource();
                ToObjectValueTarget valueTarget = new ToObjectValueTarget();
                
                for(IndexColumn indexColumn : index.getKeyColumns()) {
                    Column column = indexColumn.getColumn();
                    int colPos = column.getPosition();
                    Object objFromRow = row.get(colPos);
                    valueSource.attach(key, indexColumn);
                    final Object lastConvertedValue;
                    try {
                        lastConvertedValue = valueTarget.convertFromSource(valueSource);
                    } catch (Exception e) {
                        throw new RuntimeException("with AkType." + column.getType().akType(), e);
                    }

                    // Work around for dropping of 0 value sigfigs from key.decode()
                    int compareValue = 1;
                    if(objFromRow instanceof BigDecimal && lastConvertedValue instanceof BigDecimal) {
                        compareValue = ((BigDecimal)objFromRow).compareTo(((BigDecimal)lastConvertedValue));
                    }
                    if(compareValue != 0) {
                        if (objFromRow instanceof ByteBuffer) {
                            objFromRow = WrappingByteSource.fromByteBuffer((ByteBuffer)objFromRow);
                        }
                    }
                    ++rowCounter;
                }
            }
        });

        if(rowIt.hasNext()) {
            Assert.fail("More rows than index entries: rows("+allRows+") index("+index+")");
        }
    }

    void createAndWriteRows(int tableId, Object[] singleColumnValue) {
        int i = 0;
        for(Object o : singleColumnValue) {
            dml().writeRow(session(), createNewRow(tableId, i++, o));
        }
    }


    @Test
    public void intField() throws Exception {
        final int tid = createTableFromTypes("int");
        Integer values[] = {null, -89573, -10, 0, 1, 42, 1337, 29348291};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void intUnsignedField() throws Exception {
        final int tid = createTableFromTypes("int unsigned");
        Integer values[] = {null, 0, 1, 255, 400, 674532, 16777215, 2147483647};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void bigintUnsignedField() throws Exception {
        final int tid = createTableFromTypes("bigint unsigned");
        Long values[] = {null, 0L, 1L, 255L, 400L, 674532L, 16777215L, 2147483647L, 9223372036854775806L};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }
    
    @Test
    public void floatField() throws Exception {
        final int tid = createTableFromTypes("float");
        Float values[] = {null, -Float.MAX_VALUE, -1337.4356f, -10f, -Float.MIN_VALUE,
                          0f, Float.MIN_VALUE, 1f, 432.235f, 829483.3125f, Float.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void floatUnsignedField() throws Exception {
        final int tid = createTableFromTypes("float unsigned");
        Float values[] = {null, 0f, Float.MIN_VALUE, 1f, 42.24f, 829483.3125f, 1234567f, Float.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void doubleField() throws Exception {
        final int tid = createTableFromTypes("double");
        Double values[] = {null, -Double.MAX_VALUE, -849284.284, -5d, -Double.MIN_VALUE,
                           0d, Double.MIN_VALUE, 1d, 100d, 9128472947.284729, Double.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void doubleUnsignedField() throws Exception {
        final int tid = createTableFromTypes("double unsigned");
        Double values[] = {null, 0d, Double.MIN_VALUE, 1d, 8587d, 123456.789d, 9879679567.284729, Double.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void decimalField() throws Exception {
        final int tid = createTableFromTypes(new TypeAndParams("decimal", 5L, 2L));
        BigDecimal values[] = {null, BigDecimal.valueOf(-99999, 2), BigDecimal.valueOf(-999),
                               BigDecimal.valueOf(-1234, 1), BigDecimal.valueOf(0), BigDecimal.valueOf(1),
                               BigDecimal.valueOf(426), BigDecimal.valueOf(5678, 1), BigDecimal.valueOf(99999, 2)};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void decimalUnsignedField() throws Exception {
        final int tid = createTableFromTypes(new TypeAndParams("decimal unsigned", 5L, 2L));
        BigDecimal values[] = {null, BigDecimal.valueOf(0), BigDecimal.valueOf(1), BigDecimal.valueOf(4242, 2),
                               BigDecimal.valueOf(5678, 1), BigDecimal.valueOf(99999, 2)};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void charField() throws Exception {
        final int tid = createTableFromTypes(new TypeAndParams("char", 10L, null));
        String values[] = {null, "", "0123456789", "zebra"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void varcharField() throws Exception {
        final int tid = createTableFromTypes(new TypeAndParams("varchar", 26L, null));
        String values[] = {null, "", "abcdefghijklmnopqrstuvwxyz", "see spot run"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test(expected=UnsupportedIndexDataTypeException.class)
    public void blobField() throws Exception {
        createTableFromTypes("blob");
    }

    @Test(expected=UnsupportedIndexDataTypeException.class)
    public void textField() throws Exception {
        createTableFromTypes("text");
    }

    @Test
    public void binaryField() throws Exception {
        final int tid = createTableFromTypes(new TypeAndParams("binary", 10L, null));
        byte[][] values = {null, {}, {1,2,3,4,5}, {-24, 8, -98, 45, 67, 127, 34, -42, 9, 10}};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void varbinaryField() throws Exception {
        final int tid = createTableFromTypes(new TypeAndParams("varbinary", 26L, null));
        byte[][] values = {null, {}, {11,7,5,2}, {-24, 8, -98, 45, 67, 127, 34, -42, 9, 10, 29, 75, 127, -125, 5, 52}};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void dateField() throws Exception {
        final int tid = createTableFromTypes("date");
        String values[] = {null, "0000-00-00", "1000-01-01", "2011-05-20", "9999-12-31"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void datetimeField() throws Exception {
        final int tid = createTableFromTypes("datetime");
        String values[] = {null, "0000-00-00 00:00:00", "1000-01-01 00:00:00", "2011-05-20 17:35:01", "9999-12-31 23:59:59"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void timeField() throws Exception {
        final int tid = createTableFromTypes("time");
        String values[] = {null, "-838:59:59", "00:00:00", "17:34:20", "838:59:59"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void timestampField() throws Exception {
        final int tid = createTableFromTypes("timestamp");
        Long values[] = {null, 0L, 1305927301L, 2147483647L};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void yearField() throws Exception {
        final int tid = createTableFromTypes("year");
        String values[] = {null, "0000", "1901", "2011", "2155"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }
}
