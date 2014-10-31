/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.rowtests;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.UnsupportedIndexDataTypeException;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.store.IndexVisitor;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.WrappingByteSource;
import com.persistit.Key;
import com.persistit.Value;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KeyToObjectIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "t";
    private final boolean IS_PK = false;
    private final boolean INDEXES = true;

     private void testKeyToObject(int tableId, int expectedRowCount, String indexName) throws Exception {
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            testKeyToObjectInternal(tableId, expectedRowCount, indexName);
            txn.commit();
        }
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
    private void testKeyToObjectInternal(int tableId, int expectedRowCount, String indexName) throws Exception {
        final Table table = getTable(tableId);
        final Index index = table.getIndex(indexName);
        assertNotNull("expected index named: "+indexName, index);
        
        final List<Row> allRows = scanAll(tableId);
        assertEquals("rows scanned", expectedRowCount, allRows.size());

        final Iterator<Row> rowIt = allRows.iterator();

        store().traverse(session(), index, new IndexVisitor<Key,Value>() {
            private int rowCounter = 0;

            @Override
            protected void visit(Key key, Value value) {
                if(!rowIt.hasNext()) {
                    Assert.fail("More index entries than rows: rows("+allRows+") index("+index+")");
                }

                final Row row = rowIt.next();
                key.indexTo(0);

               
                for(IndexColumn indexColumn : index.getKeyColumns()) {
                    Column column = indexColumn.getColumn();
                    int colPos = column.getPosition();
                    Object objFromRow = ValueSources.toObject(row.value(colPos));
                    PersistitKeyValueSource valueSource = new PersistitKeyValueSource(indexColumn.getColumn().getType());
                    valueSource.attach(key, indexColumn);
                    
                    final Object lastConvertedValue;
                    try {
                        lastConvertedValue = ValueSources.toObject(valueSource);
                    } catch (Exception e) {
                        throw new RuntimeException("with type" + column.getTypeDescription(), e);
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
        }, -1, 0);

        if(rowIt.hasNext()) {
            Assert.fail("More rows than index entries: rows("+allRows+") index("+index+")");
        }
    }

    void createAndWriteRows(int tableId, Object[] singleColumnValue) {
        int i = 0;
        for(Object o : singleColumnValue) {
            writeRow(tableId, i++, o);
        }
    }


    @Test
    public void intField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ int");
        Integer values[] = {null, -89573, -10, 0, 1, 42, 1337, 29348291};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void intUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ int unsigned");
        Integer values[] = {null, 0, 1, 255, 400, 674532, 16777215, 2147483647};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void bigintUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ bigint unsigned");
        Long values[] = {null, 0L, 1L, 255L, 400L, 674532L, 16777215L, 2147483647L, 9223372036854775806L};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }
    
    @Test
    public void floatField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ float");
        Float values[] = {null, -Float.MAX_VALUE, -1337.4356f, -10f, -Float.MIN_VALUE,
                          0f, Float.MIN_VALUE, 1f, 432.235f, 829483.3125f, Float.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void floatUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ float unsigned");
        Float values[] = {null, 0f, Float.MIN_VALUE, 1f, 42.24f, 829483.3125f, 1234567f, Float.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void doubleField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ double");
        Double values[] = {null, -Double.MAX_VALUE, -849284.284, -5d, -Double.MIN_VALUE,
                           0d, Double.MIN_VALUE, 1d, 100d, 9128472947.284729, Double.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void doubleUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ double unsigned");
        Double values[] = {null, 0d, Double.MIN_VALUE, 1d, 8587d, 123456.789d, 9879679567.284729, Double.MAX_VALUE};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void decimalField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c2", "MCOMPAT_ decimal", 5L, 2L));
        BigDecimal values[] = {null, BigDecimal.valueOf(-99999, 2), BigDecimal.valueOf(-999),
                               BigDecimal.valueOf(-1234, 1), BigDecimal.valueOf(0), BigDecimal.valueOf(1),
                               BigDecimal.valueOf(426), BigDecimal.valueOf(5678, 1), BigDecimal.valueOf(99999, 2)};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void decimalUnsignedField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c2", "MCOMPAT_ decimal unsigned", 5L, 2L));
        BigDecimal values[] = {null, BigDecimal.valueOf(0), BigDecimal.valueOf(1), BigDecimal.valueOf(4242, 2),
                               BigDecimal.valueOf(5678, 1), BigDecimal.valueOf(99999, 2)};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void charField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c2", "MCOMPAT_ char", 10L, null));
        String values[] = {null, "", "0123456789", "zebra"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void varcharField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c2", "MCOMPAT_ varchar", 26L, null));
        String values[] = {null, "", "abcdefghijklmnopqrstuvwxyz", "see spot run"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test(expected=UnsupportedIndexDataTypeException.class)
    public void blobField() throws Exception {
        createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ blob");
    }

    @Test(expected=UnsupportedIndexDataTypeException.class)
    public void textField() throws Exception {
        createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ text");
    }

    @Test
    public void binaryField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c2", "MCOMPAT_ binary", 10L, null));
        byte[][] values = {null, {}, {1,2,3,4,5}, {-24, 8, -98, 45, 67, 127, 34, -42, 9, 10}};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void varbinaryField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c2", "MCOMPAT_ varbinary", 26L, null));
        byte[][] values = {null, {}, {11,7,5,2}, {-24, 8, -98, 45, 67, 127, 34, -42, 9, 10, 29, 75, 127, -125, 5, 52}};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void dateField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ date");
        String values[] = {null, "0000-00-00", "1000-01-01", "2011-05-20", "9999-12-31"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void datetimeField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ datetime");
        String values[] = {null, "0000-00-00 00:00:00", "1000-01-01 00:00:00", "2011-05-20 17:35:01", "9999-12-31 23:59:59"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void timeField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ time");
        String values[] = {null, "-838:59:59", "00:00:00", "17:34:20", "838:59:59"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void timestampField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ timestamp");
        Long values[] = {null, 0L, 1305927301L, 2147483647L};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }

    @Test
    public void yearField() throws Exception {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "MCOMPAT_ year");
        String values[] = {null, "0000", "1901", "2011", "2155"};
        createAndWriteRows(tid, values);
        testKeyToObject(tid, values.length, "c2");
    }
}
