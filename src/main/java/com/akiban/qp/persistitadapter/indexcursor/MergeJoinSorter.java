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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

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
import com.akiban.server.error.MergeSortIOException;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.tap.InOutTap;
import com.persistit.Key;
import com.persistit.KeyState;
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

    private File finalFile;

    private final SorterAdapter<?, ?, ?> sorterAdapter;
    private final List<Integer> orderChanges;
    private Key sortKey;
    private Comparator<SortKey> compare;
    
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
        
        this.sortKey = new Key ((Persistit)null);
        this.sorterAdapter = new PValueSorterAdapter();
        // Note: init may change this.ordering
        sorterAdapter.init(rowType, this.ordering, this.sortKey, null, this.context, this.bindings, sortOption);
        // Explicitly use input ordering to avoid appended field
        this.orderChanges = new ArrayList<>();
        List<Comparator<KeyState>> comparators = new ArrayList<>();
        for(int i = 0; i < ordering.sortColumns(); ++i) {
            Comparator<KeyState> c = ordering.ascending(i) ? ASC_COMPARATOR : DESC_COMPARATOR;
            if(i == 0 || ordering.ascending(i-1) != ordering.ascending(i)) {
                orderChanges.add(i);
                comparators.add(c);
            }
        }
        this.orderChanges.add(ordering.sortColumns());
        this.compare = new KeySortCompare(comparators);
        
    }

    @Override
    public RowCursor sort() {
        try {
            loadTree();
        } catch (IOException e) {
            throw new MergeSortIOException(e);
        }
        return cursor();
    }

    @Override
    public void close() {
    }
    
    private void loadTree() throws FileNotFoundException, IOException {
        MergeTempFileProvider tmpFileProvider = new MergeTempFileProvider(context);
        finalFile  = tmpFileProvider.provide();
        
        com.fasterxml.sort.Sorter<SortKey> s = new com.fasterxml.sort.Sorter<SortKey> (
                getSortConfig(tmpFileProvider),
                new KeyReaderFactory(), 
                new KeyWriterFactory(), 
                compare);
        s.sort(new KeyReadCursor(), new KeyWriter(new FileOutputStream(finalFile)));
    }
    
    private RowCursor cursor() {
        KeyFinalCursor cursor = null;
        try {
            cursor = new KeyFinalCursor (finalFile, rowType);
        } catch (FileNotFoundException e) {
            throw new MergeSortIOException(e);
        }
        return cursor;
    }

    public KeyReadCursor readCursor() { 
        return new KeyReadCursor(); 
    }
    
    private SortConfig getSortConfig (MergeTempFileProvider tmpFileProvider) {
        long maxMemory = Long.parseLong(context.getServiceManager().getConfigurationService().getProperty("akserver.sort.memory"));
        return new SortConfig().withTempFileProvider(tmpFileProvider).withMaxMemoryUsage(maxMemory);
    }
    /*
     * Base class for reading/writing bytes - 
     * KeyState[] is list of key segments broken by ASC/DESC ordering
     * rowKey is the whole, unaltered row of data. 
     */
    public static class SortKey {
        public List<KeyState> sortKeys;
        public Key rowKey;
     
        public SortKey () {
            this.sortKeys = new ArrayList<>();
            this.rowKey = new Key ((Persistit)null);
            rowKey.clear();
        }
        
        public SortKey (List<KeyState> sortKeys, Key rowKey) {
            this.sortKeys = sortKeys;
            this.rowKey = rowKey;
        }
        
        // Sorter uses size of elements to determine when the 
        // presort buffer is full. 
        public int getSize() {
            int size = 0;
            for (KeyState state : sortKeys) {
                size += state.getBytes().length + 4;
                size += 4;
            }
            size += rowKey.getEncodedSize() + 4;
            return size;
        }
    }

    private class KeyReaderFactory extends DataReaderFactory<SortKey> {

        @Override
        public DataReader<SortKey> constructReader(InputStream arg0)
                throws IOException {
            return new KeyReader(arg0);
        }
        
    }
    
    public static class KeyReader extends DataReader<SortKey> {

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
        public int estimateSizeInBytes(SortKey arg0) {
            return arg0.getSize();
        }

        @Override
        public SortKey readNext() throws IOException {
            
            SortKey key = new SortKey();
            int states = readLength();
            if (states < 0) { 
                return null;
            }
            for (int i = 0; i < states; i++) {
                KeyState state = readKeyState();
                if (state == null) {
                    return null;
                }
                key.sortKeys.add(state);
            }
            
            key.rowKey = readKey();
            
            return key;
        }

        private KeyState readKeyState() throws IOException {
            int size = readLength();
            if (size < 1) {
                return null;
            }
            byte[] bytes = new byte[size];
            int bytesRead = is.read(bytes);
            
            assert bytesRead == size: "Invalid byte count on key state read";
            
            return new KeyState(bytes);
        }
        
        private Key readKey() throws IOException{
            int size = readLength();
            if (size < 1) {
                return null;
            }
            Key key = new Key ((Persistit)null);
            key.setMaximumSize(size);
            int bytesRead = is.read(key.getEncodedBytes(), 0, size);
            
            assert bytesRead == size : "Invalid byte count on key read";

            key.setEncodedSize(size);
            return key;
        }
        
        private int readLength() throws IOException {
            length.clear();
            int bytesRead = is.read(length.array());
            if (bytesRead == -1) { // EOF marker
                return -1;
            } 
            assert bytesRead == 4 : "Invalid byte count on length read";
            return length.getInt();
        }
    }

    /*
     * Class to read rows from the input cursor to the Sort, 
     * converting them to SortKey elements for the Sorter. 
     */
    public class KeyReadCursor extends DataReader<SortKey> {
        
        private int rowCount = 0;
        private Key convertKey;
        private int rowFields;
        private TInstance tFieldTypes[];
        private PersistitKeyPValueTarget valueTarget;
        
        public KeyReadCursor () {
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
        public int estimateSizeInBytes(SortKey arg0) {
            return arg0.getSize();
        }

        @Override
        public SortKey readNext() throws IOException {
            SortKey sortKey = null;
            loadTap.in();
            try {
                Row row = input.next();
                context.checkQueryCancelation();
    
                if (row != null) {
                    ++rowCount;
                    sortKey = new SortKey (createKey(row, rowCount), createValue(row));
                }
            } finally {
                loadTap.out();
            }
            return sortKey;
        }
        
        private List<KeyState> createKey(Row row, int rowCount) {
            KeyState[] states = new KeyState[orderChanges.size() - 1];
            for(int i = 0; i < states.length; ++i) {
                int startOffset = orderChanges.get(i);
                int endOffset = orderChanges.get(i + 1);
                boolean isLast = i == states.length - 1;
                // Loop for key growth
                while(true) {
                    try {
                        sortKey.clear();
                        for(int j = startOffset; j < endOffset; ++j) {
                            sorterAdapter.evaluateToKey(row, j);
                        }
                        if(isLast && sorterAdapter.preserveDuplicates()) {
                            sortKey.append(rowCount);
                        }
                        break;
                    } catch (KeyTooLongException e) {
                        enlargeKey(sortKey);
                    }
                }
                states[i] = new KeyState(sortKey);
            }
            return Arrays.asList(states);
        }

        private Key createValue(Row row)
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
                    enlargeKey(convertKey);
                }
            }
            return new Key(convertKey);
        }
        
        private void enlargeKey (Key key) {
            if (key.getMaximumSize() == Key.MAX_KEY_LENGTH_UPPER_BOUND) {
                throw new KeyTooLongException("Maximum size exceeded=" + Key.MAX_KEY_LENGTH_UPPER_BOUND);
            }
            key.setMaximumSize(Math.min((key.getMaximumSize() * 2), Key.MAX_KEY_LENGTH_UPPER_BOUND));
        }
        
        public int rowCount() {
            return rowCount;
        }
    }
    
    private class KeyWriterFactory extends DataWriterFactory<SortKey> {

        @Override
        public DataWriter<SortKey> constructWriter(OutputStream arg0)
                throws IOException {
            return new KeyWriter(arg0);
        }
    }
    
    public static class KeyWriter extends DataWriter<SortKey> {
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
        public void writeEntry(SortKey arg0) throws IOException {
            writeInt(arg0.sortKeys.size());
            for (KeyState state : arg0.sortKeys) {
                writeKeyState (state);
            }
            writeKey (arg0.rowKey);
        }
        
        private void writeKeyState (KeyState state) throws IOException {
            writeInt (state.getBytes().length);
            os.write(state.getBytes());
        }
        
        private void writeKey (Key key) throws IOException {
            writeInt(key.getEncodedSize());
            os.write(key.getEncodedBytes(), 0, key.getEncodedSize());
        }
        
        private void writeInt (int size) throws IOException {
            length.clear();
            length.putInt(size);
            os.write(length.array());
        }
    }

    /*
     * Class to provide temporary file names for inserting the 
     * overflow buffers to disk. Implemented to the MergeJoin sort interface
     */
    public class MergeTempFileProvider implements TempFileProvider {
        
        private final File directory;
        private final String prefix;
        private final String suffix;
        public MergeTempFileProvider (QueryContext context) {
            directory = new File (context.getServiceManager().getConfigurationService().getProperty("akserver.tmp_dir"));
            suffix = ".tmp";
            String tmpPrefix;
            tmpPrefix = "sort-" +  context.getSessionId() + "-";
            prefix = tmpPrefix;
        }

        @Override
        public File provide() throws IOException {
            File f = File.createTempFile(prefix, suffix, directory);
            f.deleteOnExit();
            return f;
        }
    }
    
    /*
     * Class to create a cursor which reads the final sorted output
     * from the file, returning each sorted item as a Row. 
     */
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
            SortKey key;
            Row row = null;
            try {
                key = read.readNext();
            } catch (IOException e) {
                throw new MergeSortIOException (e);
            }
            if (key != null) {
                row = createRow (key);
                return row;
            }
            return null;
        }
        
        private Row createRow (SortKey key) {
            ValuesHolderRow rowCopy = new ValuesHolderRow(rowType, true);
            for(int i = 0 ; i < rowType.nFields(); ++i) {
                valueSources[i].attach(key.rowKey, i, valueSources[i].tInstance());
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
    
    /*
     * Comparison function, implemented for MergeSort to compare
     * the KeyState lists generated by the KeyReadCursor
     */
    public static class KeySortCompare implements Comparator<SortKey> {
        private final Comparator<KeyState>[] comparators;

        @SuppressWarnings("unchecked")
        private KeySortCompare (List<Comparator<KeyState>> comparators) {
            this.comparators = comparators.toArray(new Comparator[comparators.size()]);
        }

        @Override
        public int compare(SortKey o1, SortKey o2) {
            int val = 0;
            for (int i = 0; (i < comparators.length) && (val == 0); ++i) {
                val = comparators[i].compare(o1.sortKeys.get(i), o2.sortKeys.get(i));
            }
            return val;
        }
    }

    private static final Comparator<KeyState> ASC_COMPARATOR = new Comparator<KeyState>() {
        @Override
        public int compare(KeyState k1, KeyState k2) {
            return k1.compareTo(k2);
        }
    };

    private static final Comparator<KeyState> DESC_COMPARATOR = new Comparator<KeyState>() {
        @Override
        public int compare(KeyState k1, KeyState k2) {
            return k2.compareTo(k1);
        }
    };
}