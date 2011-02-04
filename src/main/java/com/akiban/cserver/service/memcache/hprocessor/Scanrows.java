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

package com.akiban.cserver.service.memcache.hprocessor;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.DDLFunctions;
import com.akiban.cserver.api.DDLFunctionsImpl;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.api.DMLFunctionsImpl;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.dml.scan.ColumnSet;
import com.akiban.cserver.api.dml.scan.LegacyScanRequest;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.api.dml.scan.RowDataOutput;
import com.akiban.cserver.api.dml.scan.ScanFlag;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.session.Session;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.akiban.cserver.api.HapiRequestException.ReasonCode.*;

public class Scanrows implements HapiProcessor, JmxManageable {
    private static final String MODULE = Scanrows.class.toString();
    private static final String SESSION_BUFFER = "SESSION_BUFFER";

    private final AtomicInteger bufferSize = new AtomicInteger(65535);

    private final ScanrowsMXBean bean = new ScanrowsMXBean() {
        @Override
        public int getBufferCapacity() {
            return bufferSize.get();
        }

        @Override
        public void setBufferCapacity(int bytes) {
            bufferSize.set(bytes);
        }
    };

    public static Scanrows instance() {
        return new Scanrows();
    }

    private final DDLFunctions ddlFunctions = new DDLFunctionsImpl();
    private final DMLFunctions dmlFunctions = new DMLFunctionsImpl(ddlFunctions);

    private static class RowDataStruct {
        final NewRow start;
        final NewRow end;
        final EnumSet<ScanFlag> scanFlags;

        RowDataStruct(int tableId, RowDef rowDef) {
            start = new NiceRow(tableId, rowDef);
            end = new NiceRow(tableId, rowDef);
            scanFlags = EnumSet.noneOf(ScanFlag.class);
        }

        int scanFlagsInt() {
            return ScanFlag.toRowDataFormat(scanFlags);
        }

        RowData start() {
            return start.toRowData();
        }

        RowData end() {
            return end.toRowData();
        }
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request,
                               Outputter outputter, OutputStream outputStream) throws HapiRequestException
    {
        //    public LegacyScanRequest(int tableId, RowData start, RowData end, byte[] columnBitMap, int indexId, int scanFlags)
        try {
            final int tableId;
            final Index index;
            final byte[] columnBitMap;

            TableName tableName = new TableName(request.getSchema(), request.getTable());
            Table table = ddlFunctions.getTable(session, tableName);
            tableId = table.getTableId();
            index = findIndex(table, request.getPredicates());
            columnBitMap = scanAllColumns(table);
            RowDataStruct range = getScanRange(table, request);

            LegacyScanRequest scanRequest = new LegacyScanRequest(
                    tableId, range.start(), range.end(), columnBitMap, index.getIndexId(), range.scanFlagsInt());
            List<RowData> rows = RowDataOutput.scanFull(session, dmlFunctions, getBuffer(session), scanRequest);

            outputter.output(
                    request,
                    ServiceManagerImpl.get().getStore().getRowDefCache(),
                    rows,
                    outputStream
            );

        } catch (InvalidOperationException e) {
            throw new HapiRequestException("unknown error", e); // TODO
        } catch (IOException e) {
            throw new HapiRequestException("while writing output", e, WRITE_ERROR);
        }
    }

    private ByteBuffer getBuffer(Session session) {
        ByteBuffer buffer = session.get(MODULE, SESSION_BUFFER);
        if (buffer == null) {
            buffer = ByteBuffer.allocate(bufferSize.get());
            session.put(MODULE, SESSION_BUFFER, buffer);
        }
        else {
            buffer.clear();
        }
        return buffer;
    }

    private RowDataStruct getScanRange(Table table, HapiGetRequest request)
            throws HapiRequestException, NoSuchTableException
    {
        if (request.getPredicates().size() != 1) {
            throw new UnsupportedOperationException();
        }
        HapiGetRequest.Predicate predicate = request.getPredicates().get(0);
        if (!table.getName().equals(predicate.getTableName())) {
            throw new UnsupportedOperationException();
        }
        if (!predicate.getOp().equals(HapiGetRequest.Predicate.Operator.EQ)) {
            throw new UnsupportedOperationException();
        }

        RowDataStruct ret = new RowDataStruct(table.getTableId(), ddlFunctions.getRowDef(table.getTableId()));

        ret.start.put( column(table, predicate), predicate.getValue() );
        ret.end.put( column(table, predicate), predicate.getValue() );
        ret.scanFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
        ret.scanFlags.add(ScanFlag.END_RANGE_EXCLUSIVE);
        ret.scanFlags.add(ScanFlag.DEEP);

        return ret;
    }

    private static int column(Table table, HapiGetRequest.Predicate predicate) {
        return table.getColumn(predicate.getColumnName()).getPosition();
    }

    private static byte[] scanAllColumns(Table table) {
        Set<Integer> allSet = new HashSet<Integer>();
        for (int i=0; i < table.getColumns().size(); ++i) {
            allSet.add(i);
        }
        return ColumnSet.packToLegacy(allSet);
    }

    private static List<String> predicateColumns(List<HapiGetRequest.Predicate> predicates, TableName tableName)
    throws HapiRequestException
    {
        List<String> columns = new ArrayList<String>();
        for (HapiGetRequest.Predicate predicate : predicates) {
            if (!tableName.equals(predicate.getTableName())) {
                throw new HapiRequestException(
                        String.format("predicate %s must be against table %s", predicate, tableName),
                        UNSUPPORTED_REQUEST
                );
            }
            columns.add(predicate.getColumnName());
        }
        return Collections.unmodifiableList(columns);
    }

    private interface IndexPreference {
        Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns);
    }

    private static class IdentityPreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns) {
            return candidates;
        }
    }

    private static class ColumnOrderPreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns) {
            return getBestBucket(candidates, columns, BucketSortOption.HIGHER_IS_BETTER,
                    new IndexBucketSorter() {
                        @Override
                        public int sortToBuckets(Index index, List<String> columns) {
                            int matched = 0;
                            Iterator<String> colsIter = columns.iterator();
                            for (IndexColumn iCol : index.getColumns()) {
                                String iColName = iCol.getColumn().getName();
                                if (colsIter.hasNext() && iColName.equals(colsIter.next())) {
                                    ++matched;
                                }
                                else {
                                    return matched;
                                }
                            }
                            return matched;
                        }
                    }
            );
        }
    }

    private static class UniqunessPreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns) {
            Collection<Index> unique = new ArrayList<Index>();
            Collection<Index> nonUnique = new ArrayList<Index>();
            for (Index index : candidates) {
                Collection<Index> addTo = index.isUnique() ? unique : nonUnique;
                addTo.add(index);
            }
            return unique.isEmpty() ? nonUnique : unique;
        }
    }

    private static class IndexSizePreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns) {
            return getBestBucket(candidates, columns, BucketSortOption.LOWER_IS_BETTER,
                    new IndexBucketSorter() {
                        @Override
                        public int sortToBuckets(Index index, List<String> columns) {
                            return index.getColumns().size();
                        }
                    }
            );
        }
    }

    private static class IndexNamePreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns) {
            Iterator<Index> iterator = candidates.iterator();
            Index best = iterator.next();
            while (iterator.hasNext()) {
                Index contender = iterator.next();
                if (contender.getIndexName().getName().compareTo(best.getIndexName().getName()) < 0) {
                    best = contender;
                }
            }
            return Collections.singleton(best);
        }
    }

    private interface IndexBucketSorter {
        int sortToBuckets(Index index, List<String> columns);
    }

    private enum BucketSortOption {
        HIGHER_IS_BETTER,
        LOWER_IS_BETTER
    }

    private static Collection<Index> getBestBucket(Collection<Index> candidates, List<String> columns,
                                                   BucketSortOption sortOption, IndexBucketSorter sorter)
    {
        TreeMap<Integer,Collection<Index>> buckets = new TreeMap<Integer, Collection<Index>>();
        for (Index index : candidates) {
            final int key = sorter.sortToBuckets(index, columns);
            Collection<Index> bucket = buckets.get(key);
            if (bucket == null) {
                bucket = new ArrayList<Index>();
                buckets.put(key, bucket);
            }
            bucket.add(index);
        }
        return sortOption.equals(BucketSortOption.HIGHER_IS_BETTER)
                ? buckets.lastEntry().getValue()
                : buckets.firstEntry().getValue();
    }

    private static final List<IndexPreference> PREFERENCES =
            Collections.unmodifiableList(Arrays.<IndexPreference>asList(
                    new IdentityPreference(),
                    new ColumnOrderPreference(),
                    new UniqunessPreference(),
                    new IndexSizePreference(),
                    new IndexNamePreference()
            ));

    @Override
    public Index findHapiRequestIndex(Session session, HapiGetRequest request) throws HapiRequestException {
        final Table table;
        try {
            table = ddlFunctions.getTable(session, request.getUsingTable());
        } catch (NoSuchTableException e) {
            throw new HapiRequestException("couldn't resolve table " + request.getUsingTable(), UNSUPPORTED_REQUEST);
        }
        return findIndex(table, request.getPredicates());
    }

    /**
     * Find an index that contains all of the columns specified in the predicates, and no more.
     * If the primary key matches, it is always returned. Otherwise, if more than one index matches, an
     * exception is thrown.
     * @param table the table to look in
     * @param predicates the predicates; each one's table name must match the given table
     * @return the index id
     * @throws HapiRequestException if the index can't be deduced
     */
    private static Index findIndex(Table table, List<HapiGetRequest.Predicate> predicates) throws HapiRequestException {
        List<String> columns = predicateColumns(predicates, table.getName());

        Index pk = findMatchingPK(table, columns);
        if (pk != null) {
            return pk;
        }

        Collection<Index> candidateIndexes = getCandidateIndexes(table, columns);
        if (candidateIndexes.isEmpty()) {
            throw new HapiRequestException("no valid indexes found", UNSUPPORTED_REQUEST);
        }
        for (IndexPreference preference : PREFERENCES) {
            candidateIndexes = preference.applyPreference(candidateIndexes, columns);
            if (candidateIndexes.size() == 1) {
                return candidateIndexes.iterator().next();
            }
        }
        throw new HapiRequestException("too many indexes found: " + candidateIndexes, UNSUPPORTED_REQUEST);
    }

    private static Collection<Index> getCandidateIndexes(Table table, List<String> columns) {
        List<Index> candidates = new ArrayList<Index>(table.getIndexes().size());
        for (Index possible : table.getIndexes()) {
            if (indexIsCandidate(possible, columns)) {
                candidates.add(possible);
            }
        }
        return candidates;
    }

    private static Index findMatchingPK(Table table, List<String> columns) {
        if (table instanceof UserTable) {
            UserTable utable = (UserTable) table;
            PrimaryKey pk = utable.getPrimaryKey();
            if (pk != null) {
                Index pkIndex = pk.getIndex();
                if (indexIsCandidate(pkIndex, columns)) {
                    return pkIndex;
                }
            }
        }
        return null;
    }

    static boolean indexIsCandidate(Index index, List<String> columns) {
        List<String> indexColumns = new ArrayList<String>(index.getColumns().size());
        for (IndexColumn indexColumn : index.getColumns()) {
            indexColumns.add(indexColumn.getColumn().getName());
        }
        return indexColumns.size() >= columns.size()
                && indexColumns.subList(0, columns.size()).containsAll(columns);
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("HapiP-Scanrows", bean, ScanrowsMXBean.class);
    }
}
