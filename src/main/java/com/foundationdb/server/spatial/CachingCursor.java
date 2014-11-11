package com.foundationdb.server.spatial;

import com.foundationdb.qp.operator.CursorBase;
import com.geophile.z.Cursor;
import com.geophile.z.Index;
import com.geophile.z.Record;
import com.geophile.z.space.SpaceImpl;

import java.io.IOException;

// Allows cursor to be reset to the beginning, as long as next hasn't been called at least
// CACHE_SIZE times. Useful for wrapping IndexCursorUnidirectional's for use by geophile with
// CACHE_SIZE = 1. Geophile may do a random access, then probe the same key as an ancestor
// (retrieving one record), and then probe it again to prepare for sequential accesses.

public class CachingCursor<RECORD extends Record> extends Cursor<RECORD>
{
    // Cursor interface

    @Override
    public RECORD next() throws IOException, InterruptedException
    {
        RECORD next;
        if (cachePosition < cachePositionsFilled) {
            next = cachedRecord(cachePosition++);
        } else {
            next = (RECORD) input.next();
            if (cachePosition < CACHE_SIZE) {
                recordCache[cachePosition++] = next;
                cachePositionsFilled = cachePosition;
            } else if (cachePosition == CACHE_SIZE) {
                resettable = false;
            }
        }
        return next;
    }

    @Override
    public RECORD previous() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void goTo(RECORD key) throws IOException, InterruptedException
    {
        if (key.z() != z) {
            throw new NotResettableException(key.z());
        }
        if (!resettable) {
            throw new NotResettableException();
        }
        cachePosition = 0;
    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    // CachingCursor interface

    public void open()
    {
        if (!opened) {
            input.open();
            opened = true;
        }
    }

    public void close()
    {
        input.close();
    }

    public CursorBase inputCursor()
    {
        return input;
    }

    public CachingCursor(Index<RECORD> index, long z, CursorBase<RECORD> input)
    {
        // input is actually an IndexCursorUnidirectional. But this class only uses next(),
        // and declaring CursorBase greatly simplifies testing.
        super(index);
        this.z = z;
        this.input = input;
    }

    // For use by this class

    RECORD cachedRecord(int i)
    {
        return (RECORD) recordCache[i];
    }

    // Class state

    private static final int CACHE_SIZE = 1;

    // Object state

    private final long z;
    private final CursorBase input;
    private final Object[] recordCache = new Object[CACHE_SIZE];
    private int cachePosition = 0;
    private int cachePositionsFilled = 0;
    private boolean resettable = true;
    private boolean opened = false;

    // Inner classes

    public static class NotResettableException extends RuntimeException
    {
        public NotResettableException()
        {}

        public NotResettableException(long z)
        {
            super(SpaceImpl.formatZ(z));
        }
    }
}