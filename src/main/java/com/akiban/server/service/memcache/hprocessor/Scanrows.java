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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.*;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.NoSuchTableIdException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;

import java.io.IOException;
import java.io.OutputStream;
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

import static com.akiban.server.api.HapiRequestException.ReasonCode.*;

public class Scanrows implements HapiProcessor {
    public static Scanrows instance(ConfigurationService config, DXLService dxl) {
        return new Scanrows(config, dxl);
    }

    public Scanrows(ConfigurationService config, DXLService dxl) {
        this.config = config;
        this.dxl = dxl;
    }

    private static class RowDataStruct {
        final EnumSet<ScanFlag> scanFlags;

        private final NewRow start;
        private final NewRow end;
        private final TableIndex index;

        private Integer bookendColumnId = null;
        boolean foundInequality = false;
        private final UserTable predicateTable;
        private final UserTable hRoot;

        RowDataStruct(AkibanInformationSchema ais, Index index, HapiGetRequest request)
                throws HapiRequestException
        {
            if(!index.isTableIndex()) {
                throw new IllegalArgumentException("Only TableIndex supported");
            }
            this.index = (TableIndex)index;
            int tableId = this.index.getTable().getTableId();
            final RowDef rowDef;
            try {
                rowDef = (RowDef) getTable(ais, tableId).rowDef();
            } catch (NoSuchTableException e) {
                throw new HapiRequestException("internal error; tableId" + tableId, INTERNAL_ERROR);
            }
            this.predicateTable = getUserTable(ais, request.getUsingTable());
            this.hRoot = getUserTable(ais, new TableName(request.getSchema(), request.getTable()));
            scanFlags = EnumSet.of(
                    ScanFlag.DEEP,
                    ScanFlag.START_AT_BEGINNING,
                    ScanFlag.END_AT_END
            );
            start = new NiceRow(tableId, rowDef);
            end = new NiceRow(tableId, rowDef);
        }

        private static UserTable getUserTable(AkibanInformationSchema ais, TableName tableName)
                throws HapiRequestException
        {
            try {
                Table usingTable = ais.getTable(tableName);
                if (usingTable == null) {
                    throw new HapiRequestException("no such table" + tableName, UNKNOWN_IDENTIFIER);
                }
                if (!usingTable.isUserTable()) {
                    throw new HapiRequestException("not a user table: " + tableName, UNKNOWN_IDENTIFIER);
                }
                return (UserTable) usingTable;
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

        public ColumnSelector startColumns() {
            return start.getActiveColumns();
        }

        RowData end() {
            return end.getFields().isEmpty() ? null : end.toRowData();
        }

        public ColumnSelector endColumns() {
            return end.getActiveColumns();
        }

        Table selectTable() {
            return index.getTable();
        }

        Index index() {
            return index;
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

        void ensureStartAndEndHaveSameFields()
        {
            Set<Integer> startOnly = new HashSet<Integer>(start.getFields().keySet());
            startOnly.removeAll(end.getFields().keySet());
            Set<Integer> endOnly = new HashSet<Integer>(end.getFields().keySet());
            endOnly.removeAll(start.getFields().keySet());
            for (Integer field : endOnly) {
                start.put(field, null);
            }
            for (Integer field : startOnly) {
                end.put(field, null);
            }
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

        void putEquality(HapiPredicate predicate) throws HapiRequestException {
            if (foundInequality) {
                unsupported("may not combine equality with inequality");
            }
            scanFlags.remove(ScanFlag.START_AT_BEGINNING);
            scanFlags.remove(ScanFlag.END_AT_END);

            putPredicate(predicate, start);
            putPredicate(predicate, end);
        }

        void putBookend(HapiPredicate predicate, NewRow bookendRow, ScanFlag flagToRemove)
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

        private int putPredicate(HapiPredicate predicate, NewRow row)
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

    private static Table getTable(AkibanInformationSchema ais, int tableId) {
        Table table = ais.getUserTable(tableId);
        if (table != null) {
            return table;
        }
        for (Table groupTable : ais.getGroupTables().values()) {
            Integer groupTableId = groupTable.getTableId();
            if (groupTableId != null && groupTableId == tableId) {
                return groupTable;
            }
        }
        throw new NoSuchTableIdException(tableId);
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request,
                               HapiOutputter outputter, OutputStream outputStream) throws HapiRequestException
    {
        try {
            final int knownAIS = ddlFunctions().getGeneration();
            AkibanInformationSchema ais = ddlFunctions().getAIS(session);
            validateRequest(ais, request);
            RowDataStruct range = getScanRange(ais, request);

            LegacyScanRequest scanRequest = new LegacyScanRequest(
                    range.selectTable().getTableId(),
                    range.start(),
                    range.startColumns(),
                    range.end(),
                    range.endColumns(),
                    range.columnBitmap(),
                    range.index().getIndexId(),
                    range.scanFlagsInt(),
                    configureLimit(ais, request));
            List<RowData> rows = null;
            while(rows == null) {
                rows = RowDataOutput.scanFull(session, knownAIS, dmlFunctions(), scanRequest);
            }

            outputter.output(new DefaultProcessedRequest(request, ais),
                             range.index().isHKeyEquivalent(),
                             rows,
                             outputStream
            );
        } catch (InvalidOperationException e) {
            throw new HapiRequestException("unknown error", e); // TODO
        } catch (IOException e) {
            throw new HapiRequestException("while writing output", e, WRITE_ERROR);
        }
    }

    private ScanLimit configureLimit(AkibanInformationSchema ais, HapiGetRequest request) {
        ScanLimit limit;
        // Message size limit
        int maxMessageSize = Integer.parseInt(config.getProperty("akserver.hapi.scanrows.messageSizeBytes"));
        ScanLimit messageSizeLimit = null;
        if (maxMessageSize >= 0) {
            messageSizeLimit = new MessageSizeLimit(maxMessageSize);
        }
        // Row limit
        ScanLimit countLimit = null;
        if (request.getLimit() >= 0) {
            countLimit = new PredicateLimit(ais.getTable(request.getUsingTable()).getTableId(), request.getLimit());
        }
        limit =
            messageSizeLimit == null && countLimit == null ? ScanLimit.NONE :
            messageSizeLimit == null ? countLimit :
            countLimit == null ? messageSizeLimit : new CompositeScanLimit(countLimit, messageSizeLimit);
        return limit;
    }

    private void validateRequest(AkibanInformationSchema ais, HapiGetRequest request) throws HapiRequestException {
        boolean foundSelectTable = predicateChildOfHRoot(ais, request);
        if (!foundSelectTable) {
            throw new HapiRequestException(String.format("%s is not an ancestor of %s",
                    new TableName(request.getSchema(), request.getTable()), request.getUsingTable().getTableName()),
                    UNSUPPORTED_REQUEST
            );
        }
    }

    private boolean predicateChildOfHRoot(AkibanInformationSchema ais, HapiGetRequest request) throws HapiRequestException {
        // Validate that the predicate table is a child of the hroot table
        UserTable predicateTable = ais.getUserTable(request.getUsingTable());
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

    private RowDataStruct getScanRange(AkibanInformationSchema ais, HapiGetRequest request)
            throws HapiRequestException
    {
        Index index = findHapiRequestIndex(ais, request);
        RowDataStruct ret = new RowDataStruct(ais, index, request);

        for (HapiPredicate predicate : request.getPredicates()) {
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
        ret.ensureStartAndEndHaveSameFields();
        return ret;
    }

    private static void unsupported(String message) throws HapiRequestException {
        throw new HapiRequestException(message, UNSUPPORTED_REQUEST);
    }

    private static List<String> predicateColumns(List<HapiPredicate> predicates, TableName tableName)
    throws HapiRequestException
    {
        List<String> columns = new ArrayList<String>();
        for (HapiPredicate predicate : predicates) {
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
                                          List<HapiPredicate> predicates);
    }

    private static class IdentityPreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                                 List<HapiPredicate> predicates) {
            return candidates;
        }
    }

    private static class ColumnOrderPreference implements IndexPreference {
        @Override
        public Collection<Index> applyPreference(Collection<Index> candidates, List<String> columns,
                                                 List<HapiPredicate> predicates) {
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
                                                 List<HapiPredicate> predicates) {
            for (HapiPredicate predicate : predicates) {
                if (!predicate.getOp().equals(HapiPredicate.Operator.EQ)) {
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
                                                 List<HapiPredicate> predicates) {
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
                                                 List<HapiPredicate> predicates) {
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
        return findHapiRequestIndex(ddlFunctions().getAIS(session), request);
    }

    public Index findHapiRequestIndex(AkibanInformationSchema ais, HapiGetRequest request)
            throws HapiRequestException
    {
        final UserTable table = ais.getUserTable(request.getUsingTable());
        if (table == null) {
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
        assert index.isTableIndex() : index;
        Table indexTable = ((TableIndex)index).getTable();
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

    private DDLFunctions ddlFunctions() {
        return dxl.ddlFunctions();
    }

    private DMLFunctions dmlFunctions() {
        return dxl.dmlFunctions();
    }

    private final ConfigurationService config;
    private final DXLService dxl;
}
