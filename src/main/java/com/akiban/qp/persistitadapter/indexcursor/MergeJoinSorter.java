/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.qp.persistitadapter.indexcursor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.RowCursor;
import com.akiban.qp.persistitadapter.Sorter;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyPValueTarget;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.tap.InOutTap;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.exception.KeyTooLongException;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriter;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.TempFileProvider;

/**
 * <h1>Overview</h1>
 *
 * Sort rows by inserting them in sorted order into a memory buffer, then into on-disk files, and performing 
 * a multiway merge sort on the resulting files. 
 *
 * <h1>Behavior</h1>
 *
 * The rows of the input stream are written into a memory pool (40MB as a default). When (if) the memory pool is filled,
 * the pool is written to disk, then emptied and rows are written again to the memory pool. When the final row is written
 * to the disk files are merged in an n-way merge sort. The default is 16 way sort. When all of the temp files are merged
 * into one file, the rows are read from the file in sorted order. 
 * 
 * If the initial input stream does not produce enough data to overflow the memory pool, no disk files will be produced. 
 *
 * <h1>Performance</h1>
 *
 * The MergeJoinSorter generates IO dependent upon the size of the input stream. 
 * If the input stream generates less than the memory pool size (40MB default), there is no IO generated. 
 * If the input stream generates more than the memory pool size, but less than 16x the pool size, it should
 * generate two read and two writes for each row. One write to, one read from into the initial temporary file, 
 * one write to, one read from the final sorted temporary file. For each 16x larger the input set gets it adds
 * one more write to/read from temporary file cycle.  
 *
 * <h1>Memory Requirements</h1>
 *
 * The MergeJoinSorter allocates a single memory buffer for each instance to perform an initial sort,
 * defaulting to 40MB in size. 
*/

public class MergeJoinSorter implements Sorter {

    private QueryContext context;
    private QueryBindings bindings;
    private RowCursor input;
    private RowType rowType;
    private Ordering ordering;
    private InOutTap loadTap;

    private OutputStream keyFinalFile;
    private File finalFile;
    
    public MergeJoinSorter (QueryContext context,
            QueryBindings bindings,
            RowCursor input,
            RowType rowType,
            API.Ordering ordering,
            API.SortOption sortOption,
            InOutTap loadTap)
    {
        this.context = context;
        this.bindings = bindings;
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering.copy();
        this.loadTap = loadTap;
    }

    @Override
    public RowCursor sort() {
        try {
            loadTree();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return cursor();
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }
    
    private void loadTree() throws FileNotFoundException, IOException {
        MergeTempFileProvider tmpFileProvider = new MergeTempFileProvider(context);
        finalFile  = tmpFileProvider.provide();
        
        Comparator<Key> compare = null;
        
        com.fasterxml.sort.Sorter<Key> s = new com.fasterxml.sort.Sorter<Key> (
                new SortConfig().withTempFileProvider(tmpFileProvider),
                new KeyReaderFactory(), 
                new KeyWriterFactory(), 
                compare);
        s.sort(new KeyReadCursor(context, input, rowType), new KeyWriter(new FileOutputStream(finalFile)));
       
    }
    
    private RowCursor cursor() {
        KeyFinalCursor cursor = null;
        try {
            cursor = new KeyFinalCursor (finalFile, rowType);
        } catch (FileNotFoundException e) {
            //TODO: Throw unexpected Operations exception or specific internal error
        }
        return cursor;
    }

    private class KeyReaderFactory extends DataReaderFactory<Key> {

        @Override
        public DataReader<Key> constructReader(InputStream arg0)
                throws IOException {
            return new KeyReader(arg0);
        }
        
    }
    
    public static class KeyReader extends DataReader<Key> {

        private InputStream is;
        private ByteBuffer length;
        public KeyReader (InputStream is) {
            this.is = is;
            length = ByteBuffer.allocate(4);
        }
        
        @Override
        public void close() throws IOException {
            is.close();
        }

        @Override
        public int estimateSizeInBytes(Key arg0) {
            return arg0.getEncodedSize();
        }

        @Override
        public Key readNext() throws IOException {
            length.clear();
            int bytesRead = is.read(length.array());
            if (bytesRead == -1) { // EOF marker
                return null;
            } else if (bytesRead != 4) {
                //TODO: pick an error to throw?
                return null;
            }
            int len = length.getInt();
            Key key = new Key ((Persistit)null);
            key.setMaximumSize(len);
            bytesRead = is.read(key.getEncodedBytes(), 0, len);
            if (bytesRead < len) {
                //TODO: Pick an error to throw?
                return null;
            }
            key.setEncodedSize(len);
            return key;
        }
        
    }
    
    public static class KeyReadCursor extends DataReader<Key> {
        
        private final QueryContext context;
        private RowCursor input;
        private int rowCount = 0;
        private Key convertKey;
        private int rowFields;
        private TInstance tFieldTypes[];
        private PersistitKeyPValueTarget valueTarget;
                
                
        public KeyReadCursor (QueryContext context, RowCursor input, RowType rowType) {
            this.context = context;
            this.input = input;
            this.rowFields = rowType.nFields();
            this.convertKey = new Key ((Persistit)null);
            this.tFieldTypes = new TInstance[rowFields];
            for (int i = 0; i < rowFields; i++) {
                tFieldTypes[i] = rowType.typeInstanceAt(i);
            }
            valueTarget = new PersistitKeyPValueTarget();
            valueTarget.attach(convertKey);
            
        }
        @Override
        public void close() throws IOException {
            // Do Nothing;
        }

        @Override
        public int estimateSizeInBytes(Key arg0) {
            return arg0.getEncodedSize();
        }

        @Override
        public Key readNext() throws IOException {
            Row row = input.next();
            context.checkQueryCancelation();

            if (row == null) {
                return null;
            } else {
                ++rowCount;
                createValue(row);
            }
            
            return new Key(convertKey);
        }
        
        private void createValue(Row row)
        {
            while(true) {
                try {
                    convertKey.clear();
                    for (int i = 0; i < rowFields; i++) {
                        //sorterAdapter.evaluateToTarget(row, i);
                        PValueSource field = row.pvalue(i);
                        //putFieldToTarget(field, i, oFieldTypes, tFieldTypes);
                        tFieldTypes[i].writeCanonical(field, valueTarget);
                    }
                    break;
                } catch (KeyTooLongException e) {
                    if (convertKey.getMaximumSize() == Key.MAX_KEY_LENGTH_UPPER_BOUND) {
                        throw e;
                    }
                    convertKey.setMaximumSize(Math.min((convertKey.getMaximumSize() * 2), Key.MAX_KEY_LENGTH_UPPER_BOUND));
                }
            }
        }
        public int rowCount() {
            return rowCount;
        }
    }
    
    private class KeyWriterFactory extends DataWriterFactory<Key> {

        @Override
        public DataWriter<Key> constructWriter(OutputStream arg0)
                throws IOException {
            return new KeyWriter(arg0);
        }
    }
    
    public static class KeyWriter extends DataWriter<Key> {
        private OutputStream os;
        private ByteBuffer length;

        public KeyWriter(OutputStream os) {
            this.os = os;
            length = ByteBuffer.allocate(4);
        }
        @Override
        public void close() throws IOException {
            os.close();
            
        }

        @Override
        public void writeEntry(Key arg0) throws IOException {
            length.clear();
            length.putInt(arg0.getEncodedSize());
            os.write(length.array());
            os.write(arg0.getEncodedBytes(), 0, arg0.getEncodedSize());
        }
    }
    
    public class MergeTempFileProvider implements TempFileProvider {
        
        private final File directory;
        private final String prefix;
        private final String suffix;
        public MergeTempFileProvider (QueryContext context) {
            directory = new File (context.getServiceManager().getConfigurationService().getProperty("persistit.tmpvolddir"));
            suffix = ".tmp";
            prefix = "sort-" +  context.getSessionId() + "-";
        }

        @Override
        public File provide() throws IOException {
            File f = File.createTempFile(prefix, suffix, directory);
            f.deleteOnExit();
            return f;
        }
    }
    
    public static class KeyFinalCursor implements RowCursor {
        private boolean isIdle = true;
        private boolean isDestroyed = false;

        private final KeyReader read; 
        private final RowType rowType;
        private PersistitKeyPValueSource valueSources[]; 
        
        public KeyFinalCursor (File inputFile, RowType rowType) throws FileNotFoundException {
            this (new FileInputStream(inputFile), rowType);
        }
        
        public KeyFinalCursor(InputStream stream, RowType rowType) {
            read = new KeyReader(stream);
            this.rowType = rowType;
            valueSources = new PersistitKeyPValueSource[rowType.nFields()];
            for (int i = 0; i < rowType.nFields(); i++) {
                valueSources[i] = new PersistitKeyPValueSource (rowType.typeInstanceAt(i));
            }
        }
        
        @Override
        public void open() {
            CursorLifecycle.checkIdle(this);
            isIdle = false;
        }

        @Override
        public Row next() {
            CursorLifecycle.checkIdleOrActive(this);
            Key key;
            Row row = null;
            try {
                key = read.readNext();
            } catch (IOException e) {
                //TODO: Rethrow this exception or simply return null?
                key = null;
            }
            if (key != null) {
                row = createRow (key);
                return row;
            }
            return null;
        }
        
        private Row createRow (Key key) {
            ValuesHolderRow rowCopy = new ValuesHolderRow(rowType, true);
            for(int i = 0 ; i < rowType.nFields(); ++i) {
                valueSources[i].attach(key, i, valueSources[i].tInstance());
                PValueTargets.copyFrom(valueSources[i], rowCopy.pvalueAt(i));
            }
            return rowCopy;
        }
        
        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            if(!isIdle) {
                try {
                    read.close();
                } catch (IOException e) {
                    // TODO: manage this exception? 
                }
                isIdle = true;
            }
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector) {
            throw new UnsupportedOperationException();            
        }

        @Override
        public void destroy() {
            isDestroyed = true;
        }

        @Override
        public boolean isIdle() {
            return !isDestroyed && isIdle;
        }

        @Override
        public boolean isActive() {
            return !isDestroyed && !isIdle;
        }

        @Override
        public boolean isDestroyed() {
            return isDestroyed;
        }
    }
    
    public class KeySortCompare implements Comparable<Key> {

        @Override
        public int compareTo(Key o) {
            // TODO Auto-generated method stub
            return 0;
        }
        
    }
}