package com.akiban.cserver.store;

import com.akiban.cserver.IndexDef;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.service.session.Session;
import com.persistit.exception.PersistitException;

public interface IndexManager {

    public abstract void analyzeTable(final Session session, final RowDef rowDef)
            throws Exception;

    public abstract void analyzeTable(final Session session,
            final RowDef rowDef, final int sampleSize) throws Exception;

    public abstract void deleteIndexAnalysis(final Session session,
            final IndexDef indexDef) throws PersistitException;

    public abstract void analyzeIndex(final Session session,
            final IndexDef indexDef, final int sampleSize)
            throws InvalidOperationException, PersistitException;

    public abstract void populateTableStatistics(final Session session,
            final TableStatistics tableStatistics) throws Exception;

}