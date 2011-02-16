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

package com.akiban.server.service.memcache.hprocessor;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DDLFunctionsImpl;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.DMLFunctionsImpl;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.scan.ColumnSet;
import com.akiban.server.api.dml.scan.LegacyScanRequest;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.RowDataOutput;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;

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

import static com.akiban.server.api.HapiRequestException.ReasonCode.*;

public class Scanrows implements HapiProcessor, JmxManageable {
    private static final Class<?> MODULE = Scanrows.class;
    private static final String SESSION_BUFFER = "SESSION_BUFFER";

    private static final AtomicInteger bufferSize = new AtomicInteger(65535);

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
        final EnumSet<ScanFlag> scanFlags;

        private final NewRow start;
        private final NewRow end;
        private final Index index;

        private Integer bookendColumnId = null;
        boolean foundInequality = false;
        private final UserTable predicateTable;
        private final UserTable hRoot;

        RowDataStruct(DDLFunctions ddlFunctions, Index index, HapiGetRequest request, Session session)
                throws HapiRequestException
        {
            int tableId = index.getTable().getTableId();
            final RowDef rowDef;
            try {
                rowDef = ddlFunctions.getRowDef(tableId);
            } catch (NoSuchTableException e) {
                throw new HapiRequestException("internal error; tableId" + tableId, INTERNAL_ERROR);
            }
            this.index = index;
            this.predicateTable = getUserTable(session, ddlFunctions, request.getUsingTable());
            this.hRoot = getUserTable(session, ddlFunctions, new TableName(request.getSchema(), request.getTable()));
            scanFlags = EnumSet.of(
                    ScanFlag.DEEP,
                    ScanFlag.START_AT_BEGINNING,
                    ScanFlag.END_AT_END
            );
            start = new NiceRow(tableId, rowDef);
            end = new NiceRow(tableId, rowDef);
        }

        private static UserTable getUserTable(Session session, DDLFunctions ddlFunctions,
                                              TableName tableName)
                throws HapiRequestException
        {
            try {
                Table usingTable = ddlFunctions.getTable(session, tableName);
                if (!usingTable.isUserTable()) {
                    throw new HapiRequestException("not a user table: " + tableName, UNKNOWN_IDENTIFIER);
                }
                return (UserTable) usingTable;
            } catch (NoSuchTableException e) {
                throw new HapiRequestException("no such table" + tableName, UNKNOWN_IDENTIFIER);
            } catch (ClassCastException e) {
                throw new HapiRequestException("not a UserTable: " + tableName, INTERNAL_ERROR);
            }
        }

        int scanFlagsInt() {
            return ScanFlag.toRowDataFormat(scanFlags);
        }

        RowData start() {
            return start.getFields().isEmpty() ? null : start.toRowData();
        }

        RowData end() {
            return end.getFields().isEmpty() ? null : end.toRowData();
        }

        Table selectTable() {
            return index.getTable();
        }

        int indexId() {
            return index.getIndexId();
        }

        byte[] columnBitmap() {
            if (index.getTable().isUserTable()) {
                return allColumnsBitmap(index.getTable());
            }

            Set<Integer> projection = new HashSet<Integer>();
            UserTable projectTable = predicateTable;
            while (projectTable != null) {
                addColumns(projectTable, projection);
                if (projectTable.getName().equals(hRoot.getName())) {
                    break;
                }
                projectTable = projectTable.getParentJoin().getParent();
            }
            return ColumnSet.packToLegacy(projection);
        }

        private static byte[] allColumnsBitmap(Table table) {
            Set<Integer> projection = new HashSet<Integer>();
            for (Column column : table.getColumns()) {
                projection.add(column.getPosition());
            }
            return ColumnSet.packToLegacy(projection);
        }
        private static void addColumns(UserTable table, Set<Integer> projection) {
            for (Column column : table.getColumns()) {
                projection.add(column.getGroupColumn().getPosition());
            }
        }

        void putEquality(HapiGetRequest.Predicate predicate) throws HapiRequestException {
            if (foundInequality) {
                unsupported("may not combine equality with inequality");
            }
            scanFlags.remove(ScanFlag.START_AT_BEGINNING);
            scanFlags.remove(ScanFlag.END_AT_END);

            scanFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
            scanFlags.add(ScanFlag.END_RANGE_EXCLUSIVE);

            putPredicate(predicate, start);
            putPredicate(predicate, end);
        }

        void putBookend(HapiGetRequest.Predicate predicate, NewRow bookendRow, ScanFlag flagToRemove)
                throws HapiRequestException
        {
            if(!scanFlags.remove(flagToRemove)) {
                unsupported("multiple inequality predicates must define range on a single column");
            }
            int putColumn = putPredicate(predicate, bookendRow);
            if (bookendColumnId == null) {
                bookendColumnId = putColumn;
            }
            else if (putColumn != bookendColumnId) {
                unsupported("multiple inequality predicates must define range on a single column");
            }
            foundInequality = true;
        }

        private int putPredicate(HapiGetRequest.Predicate predicate, NewRow row)
                throws HapiRequestException
        {
            Column column;
            column = predicateTable.getColumn(predicate.getColumnName());
            if (column == null) {
                unsupported("unknown column: " + predicate.getColumnName());
                throw new AssertionError("above line should have thrown an exception");
            }
            Table indexTable = index.getTable();
            if (indexTable.isGroupTable()) {
                column = column.getGroupColumn();
                if (!column.getTable().equals(predicateTable.getGroup().getGroupTable())) {
                    throw new HapiRequestException(
                            String.format("%s != %s", column.getTable(), predicateTable.getGroup().getGroupTable()),
                            INTERNAL_ERROR
                    );
                }
            }
            int pos = column.getPosition();
            row.put(pos, predicate.getValue());
            return pos;
        }
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request,
                               HapiOutputter outputter, OutputStream outputStream) throws HapiRequestException
    {
        try {
            validateRequest(session, request);
            RowDataStruct range = getScanRange(session, request);

            LegacyScanRequest scanRequest = new LegacyScanRequest(
                    range.selectTable().getTableId(),
                    range.start(),
                    range.end(),
                    range.columnBitmap(),
                    range.indexId(),
                    range.scanFlagsInt()
            );
            List<RowData> rows = RowDataOutput.scanFull(session, dmlFunctions, getBuffer(session), scanRequest);

            outputter.output(
                    new DefaultProcessedRequest(request, session, ddlFunctions),
                    rows,
                    outputStream
            );

        } catch (InvalidOperationException e) {
            throw new HapiRequestException("unknown error", e); // TODO
        } catch (IOException e) {
            throw new HapiRequestException("while writing output", e, WRITE_ERROR);
        }
    }

    private void validateRequest(Session session, HapiGetRequest request) throws HapiRequestException {
        boolean foundSelectTable = predicateChildOfHRoot(session, request);
        if (!foundSelectTable) {
            throw new HapiRequestException(String.format("%s is not an ancestor of %s",
                    new TableName(request.getSchema(), request.getTable()), request.getUsingTable().getTableName()),
                    UNSUPPORTED_REQUEST
            );
        }
    }

    private boolean predicateChildOfHRoot(Session session, HapiGetRequest request) throws HapiRequestException {
        // Validate that the predicate table is a child of the hroot table
        UserTable predicateTable = ddlFunctions.getAIS(session).getUserTable(request.getUsingTable());
        if (predicateTable == null) {
            throw new HapiRequestException("unknown predicate table: " + request.getUsingTable(), UNKNOWN_IDENTIFIER);
        }

        if (predicateTable.getName().equals(request.getSchema(), request.getTable())) {
            return true;
        }

        UserTable child = predicateTable;
        while (child.getParentJoin() != null) {
            UserTable parent = child.getParentJoin().getParent();
            TableName parentName = parent.getName();
            if (!parentName.getSchemaName().equals(request.getSchema())) {
                throw new HapiRequestException("group spans multiple schemas", UNSUPPORTED_REQUEST);
            }
            if (parentName.getTableName().equals(request.getTable())) {
                return true;
            }
            child = parent;
        }

        return false;
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

    private RowDataStruct getScanRange(Session session, HapiGetRequest request)
            throws HapiRequestException, NoSuchTableException
    {
        Index index = findHapiRequestIndex(session, request);
        RowDataStruct ret = new RowDataStruct(ddlFunctions, index, request, session);

        for (HapiGetRequest.Predicate predicate : request.getPredicates()) {
            switch (predicate.getOp()) {
                case EQ:
                    ret.putEquality(predicate);
                    break;
                case GT:
                    ret.scanFlags.add(ScanFlag.START_RANGE_EXCLUSIVE);
                    // fall through
                case GTE:
                    ret.putBookend(predicate, ret.start, ScanFlag.START_AT_BEGINNING);
                    break;
                case LT:
                    ret.scanFlags.add(ScanFlag.END_RANGE_EXCLUSIVE);
                    // fall through
                case LTE:
                    ret.putBookend(predicate, ret.end, ScanFlag.END_AT_END);
                    break;
                default:
                    unsupported(predicate.getOp() + " not supported");
                    break;
            }
        }

        return ret;
    }

    private static void unsupported(String message) throws HapiRequestException {
        throw new HapiRequestException(message, UNSUPPORTED_REQUEST);
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
        Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                          List<HapiGetRequest.Predicate> predicates);
    }

    private static class IdentityPreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                                 List<HapiGetRequest.Predicate> predicates) {
            return candidates;
        }
    }

    private static class ColumnOrderPreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                                 List<HapiGetRequest.Predicate> predicates) {
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
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                                 List<HapiGetRequest.Predicate> predicates) {
            for (HapiGetRequest.Predicate predicate : predicates) {
                if (!predicate.getOp().equals(HapiGetRequest.Predicate.Operator.EQ)) {
                    return candidates;
                }
            }
            Collection<Index> unique = new ArrayList<Index>();
            Collection<Index> nonUnique = new ArrayList<Index>();
            for (Index index : candidates) {
                final Collection<Index> addTo;
                if (index.isUnique()) {
                    addTo = indexColumns(index).containsAll(columns) ? unique : nonUnique;
                }
                else {
                    addTo = nonUnique;
                }
                addTo.add(index);
            }
            return unique.isEmpty() ? nonUnique : unique;
        }
    }

    private static class IndexSizePreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                                 List<HapiGetRequest.Predicate> predicates) {
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
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                                 List<HapiGetRequest.Predicate> predicates) {
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
    public Index findHapiRequestIndex(Session session, HapiGetRequest request)
            throws HapiRequestException
    {
        final UserTable table;
        try {
            table = ddlFunctions.getUserTable(session, request.getUsingTable());
        } catch (NoSuchTableException e) {
            throw new HapiRequestException("couldn't resolve table " + request.getUsingTable(), UNSUPPORTED_REQUEST);
        }
        boolean useGroupTable = !request.getUsingTable().equals(request.getSchema(), request.getTable());

        List<String> columns = predicateColumns(request.getPredicates(), table.getName());

        Index pk = findMatchingPK(table, columns);
        if (pk != null) {
            return useGroupTable ? getGroupTableIndex(pk) : pk;
        }

        Collection<Index> candidateIndexes = getCandidateIndexes(table, columns);
        if (candidateIndexes.isEmpty()) {
            throw new HapiRequestException("no valid indexes found", UNSUPPORTED_REQUEST);
        }
        for (IndexPreference preference : PREFERENCES) {
            candidateIndexes = preference.applyPreference(candidateIndexes, columns, null);
            if (candidateIndexes.size() == 1) {
                Index index = candidateIndexes.iterator().next();
                return useGroupTable ? getGroupTableIndex(index) : index;
            }
        }
        // We shouldn't ever get here
        throw new AssertionError("too many indexes: " + candidateIndexes);
    }

    private static Index getGroupTableIndex(Index index) throws HapiRequestException {
        Table indexTable = index.getTable();
        if (!indexTable.isUserTable()) {
            throw new HapiRequestException("not a user table: " + indexTable, UNKNOWN_IDENTIFIER);
        }
        UserTable uTable = (UserTable) indexTable;
        List<Index> groupIndexes = new ArrayList<Index>();
        int uIndexId = index.getIndexId();
        for (Index groupIndex : uTable.getGroup().getGroupTable().getIndexes()) {
            if (groupIndex.getIndexId() == uIndexId) {
                groupIndexes.add(groupIndex);
            }
        }
        if (groupIndexes.size() != 1) {
            throw new HapiRequestException("not 1 group index: " + groupIndexes, INTERNAL_ERROR);
        }
        return groupIndexes.get(0);
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

    private static Index findMatchingPK(UserTable utable, List<String> columns) {
        PrimaryKey pk = utable.getPrimaryKey();
        if (pk != null) {
            Index pkIndex = pk.getIndex();
            if (indexIsCandidate(pkIndex, columns)) {
                return pkIndex;
            }
        }
        return null;
    }

    static boolean indexIsCandidate(Index index, List<String> columns) {
        List<String> indexColumns = indexColumns(index);
        return indexColumns.size() >= columns.size()
                && indexColumns.subList(0, columns.size()).containsAll(columns);
    }

    private static List<String> indexColumns(Index index) {
        List<String> indexColumns = new ArrayList<String>(index.getColumns().size());
        for (IndexColumn indexColumn : index.getColumns()) {
            indexColumns.add(indexColumn.getColumn().getName());
        }
        return indexColumns;
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("HapiP-Scanrows", bean, ScanrowsMXBean.class);
    }
}
