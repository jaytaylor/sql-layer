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

package com.foundationdb.qp.persistitadapter.indexcursor;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.Merger;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.SortingState;
import com.fasterxml.sort.util.SegmentedBuffer;
import com.foundationdb.server.error.AkibanInternalException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Stock Sorter is push based, expected an output stream upon write.
 *
 * To use in the Operator stack we would need to start another thread or write
 * the sort output to a temporary file. The former seems excessive and the
 * latter seems wasteful, both for small sorts (going to disk when it fits in
 * memory buffer) and for large sorts (double writing and reading the entire
 * sort stream).
 *
 * This class duplicates a bit of Sorter but short circuits the default output
 * so that it can be used in a pull fashion.
 */
public class PullBasedSorter<T> extends Sorter<T> implements PullNextProvider<T>
{
    private static final Method readMaxMethod;

    static {
        try {
            readMaxMethod = Sorter.class.getDeclaredMethod(
                "_readMax",
                DataReader.class,       // DataReader<T>
                SegmentedBuffer.class,
                long.class,
                Object.class            // T
            );
            readMaxMethod.setAccessible(true);
        } catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    // Only set if merge was without spilling to disk
    private Object[] _items;
    private int _itemsIndex;
    // Only set if merge spilled to disk
    private DataReader<T> _merger;
    private List<File> _inputs;


    public PullBasedSorter(SortConfig config,
                           DataReaderFactory<T> readerFactory,
                           DataWriterFactory<T> writerFactory,
                           Comparator<T> comparator) {
        super(config, readerFactory, writerFactory, comparator);
    }


    /**
     * Like Sorter.sort() but don't do any output.
     * Prepares the sorted stream to be read incrementally with {@link #pullNext()}
     * Must close {@link #pullFinish()}} when done with the sort.
     */
    public boolean pullSortBegin(DataReader<T> inputReader) throws IOException {
        // Clean up any previous pull sort
        pullFinish();

        // First, pre-sort:
        _phase = SortingState.Phase.PRE_SORTING;

        SegmentedBuffer buffer = new SegmentedBuffer();
        boolean inputClosed = false;

        _presortFileCount = 0;
        _sortRoundCount = -1;
        _currentSortRound = -1;

        try {
            try {
                _items = (Object[])readMaxMethod.invoke(this, inputReader, buffer, _config.getMaxMemoryUsage(), null);
            } catch(IllegalAccessException | InvocationTargetException e) {
                throw new AkibanInternalException("Invocation error", e);
            }
            if(_checkForCancel()) {
                return false;
            }
            Arrays.sort(_items, _rawComparator());
            T next = inputReader.readNext();
            /* Minor optimization: in case all entries might fit in
             * in-memory sort buffer, avoid writing intermediate file
             * and just write results directly.
             */
            if(next == null) {
                inputClosed = true;
                inputReader.close();
                _phase = SortingState.Phase.SORTING;
                _itemsIndex = 0;
            } else { // but if more data than memory-buffer-full, do it right:
                List<File> presorted = new ArrayList<>();
                presorted.add(_writePresorted(_items));
                _items = null; // it's a big array, clear refs as early as possible
                _presort(inputReader, buffer, next, presorted);
                inputClosed = true;
                inputReader.close();
                _phase = SortingState.Phase.SORTING;
                if(_checkForCancel(presorted)) {
                    return false;
                }
                pullMerge(presorted);
            }
            if(_checkForCancel()) {
                return false;
            }
            _phase = SortingState.Phase.COMPLETE;
        } finally {
            if(!inputClosed) {
                try {
                    inputReader.close();
                } catch(IOException e) {
                    // Ignore
                }
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    /** Like writeAll() from fast path in Sorter.sort() and Sorter._merge() in spilled path. */
    public T pullNext() throws IOException {
        if(_items != null) {
            assert _merger == null;
            if(_itemsIndex < _items.length) {
                return (T)_items[_itemsIndex++];
            }
        } else {
            assert _merger != null;
            return _merger.readNext();
        }
        return null;
    }

    @Override
    public void pullFinish() throws IOException {
        if(_items != null) {
            _items = null;
        }
        _itemsIndex = -1;
        if(_merger != null) {
            _merger.close();
        }
        if(_inputs != null) {
            for(File input : _inputs) {
                input.delete();
            }
        }
    }


    //
    // Internal
    //

    /** Like Sorter.merge() + Sorter._merge(), without closing inputs */
    private void pullMerge(List<File> presorted) throws IOException {
        // Ok, let's see how many rounds we should have...
        final int mergeFactor = _config.getMergeFactor();
        _sortRoundCount = _calculateRoundCount(presorted.size(), mergeFactor);
        _currentSortRound = 0;

        // first intermediate rounds
        _inputs = presorted;
        while(_inputs.size() > mergeFactor) {
            ArrayList<File> outputs = new ArrayList<>(1 + ((_inputs.size() + mergeFactor - 1) / mergeFactor));
            for(int offset = 0, end = _inputs.size(); offset < end; offset += mergeFactor) {
                int localEnd = Math.min(offset + mergeFactor, end);
                outputs.add(_merge(_inputs.subList(offset, localEnd)));
            }
            ++_currentSortRound;
            // and then switch result files to be input files
            _inputs = outputs;
        }
        // and create the merger
        ArrayList<DataReader<T>> readers = new ArrayList<>(_inputs.size());
        for(File mergedInput : _inputs) {
            readers.add(_readerFactory.constructReader(new FileInputStream(mergedInput)));
        }
        _merger = Merger.mergedReader(_comparator, readers);
    }
}
