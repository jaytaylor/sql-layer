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

package com.foundationdb.qp.storeadapter.indexcursor;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.valuesScan_Default;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.fasterxml.sort.IterableSorterException;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.KeyReader;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.KeyReadCursor;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.KeyWriter;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.KeyFinalCursor;
import com.foundationdb.qp.storeadapter.indexcursor.MergeJoinSorter.SortKey;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.test.it.qp.OperatorITBase;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.util.tap.Tap;

public class KeyFinalCursorIT extends OperatorITBase
{
    private Schema schema;
    private ByteArrayOutputStream os;
    private ByteArrayInputStream is;
    private SortKey startKey; 
    private KeyWriter writer;
    private List<BindableRow> bindRows;
    private final Random random = new Random (100);
    
    @Before
    public void createFileBuffers() {
        schema = new Schema(ais());
        os = new ByteArrayOutputStream();
        startKey = new SortKey();
        writer = new KeyWriter(os);
        bindRows = new ArrayList<>();

    }

    @Test
    public void cycleComplete() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(true));
        
        Row[] rows = new Row[] {
                row(rowType, 1L),
        };
        
        bindRows.add(BindableRow.of(rows[0]));
    
        RowCursor cursor = cycleRows(rowType);
        compareRows(rows, cursor);
    }

    @Test
    public void cycleNValues() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(false),MNumeric.INT.instance(true), MString.varchar());
        Row[] rows = new Row[] {
                row(rowType, 1L, 100L, "A"),
        };
        
        bindRows.add(BindableRow.of(rows[0]));
        RowCursor cursor = cycleRows (rowType);
        compareRows(rows, cursor);
    }
    
    @Test
    public void cycleNRows() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(true));
        
        List<Row> rows = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            Row row = row (rowType, i);
            rows.add(row);
            bindRows.add(BindableRow.of(row));
        }
        RowCursor cursor = cycleRows (rowType);
        
        Row[] rowArray = new Row[rows.size()];
        compareRows (rows.toArray(rowArray), cursor); 
    }
    
    @Test
    public void cycleManyRows() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(false), MNumeric.INT.instance(true), MString.varchar());
        List<Row> rows = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            Row row = row (rowType, random.nextInt(), i, 
                    characters(5+random.nextInt(1000)));
            rows.add(row);
            bindRows.add(BindableRow.of(row));
        }
        RowCursor cursor = cycleRows(rowType);
        Row[] rowArray = new Row[rows.size()];
        compareRows (rows.toArray(rowArray), cursor);
        
    }
    
    @Test
    public void cycleDecimalRows() throws IOException {
        RowType rowType = schema.newValuesType(MNumeric.INT.instance(false), MNumeric.DECIMAL.instance(11,0, false), MString.varchar());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BigDecimal value = new BigDecimal (random.nextInt(100000));
            rows.add(row(rowType, random.nextInt(), value, characters(5+random.nextInt(10))));
            bindRows.add(BindableRow.of(rows.get(i)));
        }
        
        RowCursor cursor = cycleRows(rowType);
        Row[] rowArray = new Row[rows.size()];
        compareRows (rows.toArray(rowArray), cursor);
    }
    
    @Test
    public void cycleBlobRows() throws IOException {
        RowType rowType = schema.newValuesType(MString.TEXT.instance(false), MString.TINYTEXT.instance(false));
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows.add(row(rowType, characters(5+random.nextInt(100)), characters(5+random.nextInt(10))));
            bindRows.add(BindableRow.of(rows.get(i)));
        }
        RowCursor cursor = cycleRows(rowType);
        Row[] rowArray = new Row[rows.size()];
        compareRows (rows.toArray(rowArray), cursor);
    }
    
    private RowCursor cycleRows(RowType rowType) throws IOException {
        KeyReadCursor keyCursor = getKeyCursor(rowType , bindRows);

        startKey  = keyCursor.readNext(); 
        while (startKey != null) {
            writer.writeEntry(startKey);
            startKey = keyCursor.readNext();
        }
        
        is = new ByteArrayInputStream (os.toByteArray());
        return new KeyFinalCursor(new StreamIterator(is), rowType, API.SortOption.PRESERVE_DUPLICATES, null);

    }
    
    private KeyReadCursor getKeyCursor (RowType rowType, List<BindableRow> rows) {

        Operator op = valuesScan_Default(rows, rowType);
        Cursor cursor = cursor(op, queryContext, queryBindings);
        API.Ordering ordering = API.ordering();
        ordering.append(new TPreparedField (rowType.typeAt(0), 0), true);
        
        MergeJoinSorter mergeSorter = new MergeJoinSorter(queryContext, queryBindings, cursor, 
                rowType, ordering, API.SortOption.PRESERVE_DUPLICATES, Tap.createTimer("Test Tap"));
        
        cursor.open();
        return mergeSorter.readCursor();
    }
    static final String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public String characters(final int length) {
        StringBuilder sb = new StringBuilder(length);
        for( int i = 0; i < length; i++ ) 
           sb.append(ALPHA.charAt(random.nextInt(ALPHA.length())));
        return sb.toString();
     }


    private static class StreamIterator implements Iterator<SortKey> {
        private final KeyReader reader;
        private SortKey next;

        public StreamIterator(InputStream stream) throws IOException {
            this.reader = new KeyReader(stream);
            this.next = reader.readNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public SortKey next() {
            if(next == null) {
                throw new IllegalStateException("No next");
            }
            SortKey t = next;
            try {
                next = reader.readNext();
                if(next == null) {
                    reader.close();
                }
            } catch(IOException e) {
                throw new IterableSorterException(e);
            }
            return t;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
