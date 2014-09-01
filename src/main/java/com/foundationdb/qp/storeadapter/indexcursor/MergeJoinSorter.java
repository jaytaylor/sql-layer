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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.sort.IterableSorterException;
import com.fasterxml.sort.IteratingSorter;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.PersistitValueValueSource;
import com.foundationdb.server.PersistitValueValueTarget;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.MergeSortIOException;
import com.foundationdb.server.error.StorageKeySizeExceededException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.tap.InOutTap;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;
import com.persistit.Value;
import com.persistit.exception.ConversionException;
import com.persistit.exception.KeyTooLongException;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriter;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.TempFileProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1>Overview</h1>
 *
 * Sort rows by inserting them in sorted order into a memory buffer, then into on-disk files, and performing 
 * a multi-way merge sort on the resulting files.
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
    private static final Logger LOG = LoggerFactory.getLogger(MergeJoinSorter.class);

    private QueryContext context;
    private QueryBindings bindings;
    private RowCursor input;
    private RowType rowType;
    private Ordering ordering;
    private InOutTap loadTap;

    private final SorterAdapter<?, ?, ?> sorterAdapter;
    private final List<Integer> orderChanges;
    private IteratingSorter<SortKey> iteratingSorter;
    private Iterator<SortKey> sortIterator;
    private Key sortKey;
    private Comparator<SortKey> compare;
    private API.SortOption sortOption;
    
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
        this.sortOption = sortOption;
        
        this.sortKey = context.getStore().createKey(); 
        this.sorterAdapter = new ValueSorterAdapter();
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
        if(iteratingSorter != null) {
            iteratingSorter.close();
            iteratingSorter = null;
            sortIterator = null;
        }
    }
    
    private void loadTree() throws IOException {
        iteratingSorter = new IteratingSorter<>(getSortConfig(new MergeTempFileProvider(context)),
                                                new KeyReaderFactory(),
                                                new KeyWriterFactory(),
                                                compare);
        sortIterator = iteratingSorter.sort(new KeyReadCursor(input));
    }
    
    private RowCursor cursor() {
        return new KeyFinalCursor(sortIterator, rowType, sortOption, compare);
    }

    public KeyReadCursor readCursor() { 
        return new KeyReadCursor(input); 
    }
    
    private SortConfig getSortConfig (MergeTempFileProvider tmpFileProvider) {
        long maxMemory = Long.parseLong(context.getServiceManager().getConfigurationService().getProperty("fdbsql.sort.memory"));
        return new SortConfig().withTempFileProvider(tmpFileProvider).withMaxMemoryUsage(maxMemory);
    }
    /*
     * Base class for reading/writing bytes - 
     * KeyState[] is list of key segments broken by ASC/DESC ordering
     * rowKey is the whole, unaltered row of data. 
     */
    public static class SortKey {
        public List<KeyState> sortKeys;
        public Value rowValue;
     
        public SortKey () {
            this.sortKeys = new ArrayList<>();
            this.rowValue = new Value((Persistit)null);
            rowValue.clear();
        }
        
        public SortKey (List<KeyState> sortKeys, Value value) {
            this.sortKeys = sortKeys;
            this.rowValue = value;
        }
        
        // Sorter uses size of elements to determine when the 
        // presort buffer is full. 
        public int getSize() {
            int size = 0;
            for (KeyState state : sortKeys) {
                size += state.getBytes().length + 4;
                size += 4;
            }
            size += rowValue.getEncodedSize() + 4;
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
    
    /*
     * Class to reak keys from the sorter (i.e. from disk)
     * back into memory for processing. 
     */
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
            
            key.rowValue = readValue();
            
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
        
        private Value readValue() throws IOException {
            int size = readLength();
            if (size < 1) { return null; }
            Value value = new Value ((Persistit)null);
            value.setMaximumSize(size);
            value.ensureFit(size);
            int bytesRead = is.read(value.getEncodedBytes(), 0, size);
            assert bytesRead == size : "Invalid byte count on value read";
            value.setEncodedSize(size);
            return value;
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
        private int rowFields;
        private TInstance tFieldTypes[];
        private AkCollator collators[];
        private PersistitValueValueTarget valueTarget;
        private RowCursor input;
        boolean done = false;
        
        public KeyReadCursor (RowCursor input) {
            this.rowFields = rowType.nFields();
            this.tFieldTypes = new TInstance[rowFields];
            this.collators = new AkCollator[rowFields];
            for (int i = 0; i < rowFields; i++) {
                tFieldTypes[i] = rowType.typeAt(i);
                if (tFieldTypes[i] != null && tFieldTypes[i].typeClass() instanceof TString) {
                    collators[i] = TString.getCollator(tFieldTypes[i]);
                }
            }
            valueTarget = new PersistitValueValueTarget();
            this.input = input;
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
        public SortKey readNext() {
            SortKey sortKey = null;
            if(done) {
                return sortKey;
            }
            loadTap.in();
            try {
 
                Row row = input.next();
                context.checkQueryCancelation();
    
                if (row != null) {
                    ++rowCount;
                    sortKey = new SortKey (createKey(row, rowCount), createValue(row));
                } else {
                    done = true;
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
                    } catch (KeyTooLongException | StorageKeySizeExceededException e) {
                        enlargeKey(sortKey);
                    }
                }
                states[i] = new KeyState(sortKey);
            }
            return Arrays.asList(states);
        }

        private static final int SIZE_GRANULARITY = 256;
        private Value createValue(Row row)
        {
            // Do a rough calculation of size of the row data
            int size = 0;
            for (int i = 0; i < rowFields; i++) {
                if (tFieldTypes[i] == null) {
                    size += 1;
                } else if (tFieldTypes[i].typeClass().hasFixedSerializationSize()) {
                    size += tFieldTypes[i].typeClass().fixedSerializationSize() + 2;
                } else {
                    ValueSource src = row.value(i);
                    if (!src.isNull()) {
                        switch (TInstance.underlyingType(src.getType())) {
                        case STRING:
                            size += AkCollator.getDebugString(src, collators[i]).length() * 2 + 3;
                            break;
                        case BYTES:
                            size += src.getBytes().length;
                            break;
                        default:
                            throw new IllegalArgumentException("Unexpected UnderlyingType: " + src.getType());
                        }
                    } else {
                        size += 1;
                    }
                }
            }
            size = ((size  + SIZE_GRANULARITY - 1) / SIZE_GRANULARITY) * SIZE_GRANULARITY;
            
            // Create a new conversion value 
            Value convertValue =  new Value ((Persistit)null, 
                        Math.max(size, Value.INITIAL_SIZE), 
                        Math.max(size, Value.DEFAULT_MAXIMUM_SIZE));
            valueTarget.attach(convertValue);            
            // Covert the row to the Value for storage in the SortKey
            while(true) {
                try {
                    convertValue.clear();
                    convertValue.setStreamMode(true);
                    for (int i = 0; i < rowFields; i++) {
                        ValueSource field = row.value(i);
                        if (field.isNull()) {
                            valueTarget.putNull();
                        } else {
                            tFieldTypes[i].writeCanonical(field, valueTarget);
                        }
                    }
                    break;
                } catch (ConversionException e) {
                    enlargeValue(convertValue);
                }
            }
            // reset some more un-needed internal state. But this requires 
            // making a copy of the internal data, again. 
            return new Value(convertValue);
        }
        
        private void enlargeValue (Value value) {
            if (value.getMaximumSize() == Value.MAXIMUM_SIZE) {
                throw new KeyTooLongException("Maximum size exceeded=" + Value.MAXIMUM_SIZE);
            }
            value.setMaximumSize(Math.min(value.getMaximumSize() *2, Value.MAXIMUM_SIZE));
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
            writeKey (arg0.rowValue);
        }
        
        private void writeKeyState (KeyState state) throws IOException {
            writeInt (state.getBytes().length);
            os.write(state.getBytes());
        }
        
        private void writeKey (Value key) throws IOException {
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
            directory = new File (context.getServiceManager().getConfigurationService().getProperty("fdbsql.tmp_dir"));
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

        private final Iterator<SortKey> sortIterator;
        private final RowType rowType;
        private PersistitValueValueSource valueSource;
        private API.SortOption sortOption;
        private Comparator<SortKey> compare;
        private SortKey nextKey;
        
        public KeyFinalCursor(Iterator<SortKey> sortIterator, RowType rowType, API.SortOption sortOption, Comparator<SortKey> compare) {
            this.sortIterator = sortIterator;
            this.rowType = rowType;
            this.sortOption = sortOption;
            this.compare = compare;
            valueSource = new PersistitValueValueSource();
        }
        
        @Override
        public void open() {
            CursorLifecycle.checkIdle(this);
            isIdle = false;
        }

        @Override
        public Row next() {
            CursorLifecycle.checkIdleOrActive(this);
            Row row = null;
            try {
                SortKey key = (nextKey != null) ? nextKey : (sortIterator.hasNext() ? sortIterator.next() : null);
                if(key != null) {
                    if(sortOption == API.SortOption.SUPPRESS_DUPLICATES) {
                        nextKey = skipDuplicates(key);
                    }
                    row = createRow(key);
                }
            } catch(IterableSorterException e) {
                throw new MergeSortIOException(e.getMessage());
            }
            return row;
        }

        /*
         * The FasterXML.Sort isn't capable of removing duplicates, it just 
         * puts them in order in the sort output. Skip the duplicates by reading
         * until the end of the stream or until a different key appears 
         */
        private SortKey skipDuplicates (SortKey startKey) {
            while (true) {
                SortKey newKey = sortIterator.hasNext() ? sortIterator.next() : null;
                if (newKey == null || compare.compare(startKey, newKey) != 0) {
                    return newKey;
                }
            }
        }
        
        private Row createRow (SortKey key) {
            ValuesHolderRow rowCopy = new ValuesHolderRow(rowType);
            valueSource.attach(key.rowValue);
            for(int i = 0 ; i < rowType.nFields(); ++i) {
                valueSource.getReady(rowType.typeAt(i));
                if (valueSource.isNull()) {
                    rowCopy.valueAt(i).putNull();
                } else {
                    rowType.typeAt(i).writeCanonical(valueSource, rowCopy.valueAt(i));
                }
            }
            return rowCopy;
        }
        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            if(!isIdle) {
                isIdle = true;
                // pullSorter closed by MergeJoinSorter
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
