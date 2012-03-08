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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.store.statistics.IndexStatistics;
import static com.akiban.server.store.statistics.IndexStatistics.*;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.Persistit;

import com.google.common.primitives.UnsignedBytes;

import java.util.*;

public abstract class CostEstimator
{
    private final Key key;
    private final PersistitKeyValueTarget keyTarget;
    private final Comparator<byte[]> bytesComparator;

    protected CostEstimator() {
        key = new Key((Persistit)null);
        keyTarget = new PersistitKeyValueTarget();
        bytesComparator = UnsignedBytes.lexicographicalComparator();
    }

    public abstract long getTableRowCount(Table table);
    public abstract IndexStatistics getIndexStatistics(Index index);

    // TODO: These need to be figured out for real.
    public static final double RANDOM_ACCESS_COST = 1.25;
    public static final double SEQUENTIAL_ACCESS_COST = 1.0;
    public static final double FIELD_ACCESS_COST = .01;
    public static final double SORT_COST = 2.0;
    public static final double SELECT_COST = .25;

    /** Estimate cost of scanning from this index. */
    public CostEstimate costIndexScan(Index index,
                                      List<ExpressionNode> equalityComparands,
                                      ExpressionNode lowComparand, boolean lowInclusive,
                                      ExpressionNode highComparand, boolean highInclusive) {
        if (index.isUnique()) {
            if ((equalityComparands != null) &&
                (equalityComparands.size() == index.getKeyColumns().size())) {
                // Exact match from unique index; probably one row.
                return indexAccessCost(1, index);
            }
        }
        long rowCount = getTableRowCount(index.leafMostTable());
        long statsCount = 0;
        IndexStatistics indexStats = getIndexStatistics(index);
        if (indexStats != null)
            statsCount = indexStats.getRowCount();
        int columnCount = 0;
        if (equalityComparands != null)
            columnCount = equalityComparands.size();
        if ((lowComparand != null) || (highComparand != null))
            columnCount++;
        Histogram histogram;
        if ((statsCount == 0) ||
            (columnCount == 0) ||
            ((histogram = indexStats.getHistogram(columnCount)) == null)) {
            // No stats or just used for ordering.
            // TODO: Is this too conservative?
            return indexAccessCost(rowCount, index);
        }
        boolean scaleCount = true;
        long nrows;
        if ((lowComparand == null) && (highComparand == null)) {
            // Equality lookup.

            // If a histogram is almost unique, and in particular unique but
            // not so declared, then the result size doesn't scale up from
            // when it was analyzed.
            long totalDistinct = histogram.totalDistinctCount();
            scaleCount = totalDistinct * 9 < statsCount * 10; // < 90% distinct
            byte[] keyBytes = encodeKeyBytes(index, equalityComparands, null);
            if (keyBytes == null) {
                // Variable.
                nrows = (scaleCount) ? statsCount / totalDistinct : 1;
            }
            else {
                nrows = rowsEqual(histogram, keyBytes);
            }
        }
        else {
            byte[] lowBytes = encodeKeyBytes(index, equalityComparands, lowComparand);
            byte[] highBytes = encodeKeyBytes(index, equalityComparands, highComparand);
            if ((lowBytes == null) && (highBytes == null)) {
                // Range completely unknown.
                nrows = indexStats.getRowCount();
            }
            else {
                nrows = rowsBetween(histogram, lowBytes, lowInclusive, highBytes, highInclusive);
            }
        }
        if (scaleCount)
            nrows = simpleRound((nrows * rowCount), statsCount);
        return indexAccessCost(nrows, index);
    }

    // Estimate cost of fetching nrows from index.
    // One random access to get there, then nrows-1 sequential accesses following,
    // Plus a surcharge for copying something as wide as the index.
    private static CostEstimate indexAccessCost(long nrows, Index index) {
        return new CostEstimate(nrows, 
                                RANDOM_ACCESS_COST +
                                ((nrows - 1) * SEQUENTIAL_ACCESS_COST) +
                                nrows * FIELD_ACCESS_COST * index.getKeyColumns().size());
    }

    protected long rowsEqual(Histogram histogram, byte[] keyBytes) {
        // TODO; Could use Collections.binarySearch if we had
        // something that looked like a HistogramEntry.
        List<HistogramEntry> entries = histogram.getEntries();
        for (HistogramEntry entry : entries) {
            int compare = bytesComparator.compare(keyBytes, entry.getKeyBytes());
            if (compare == 0)
                return entry.getEqualCount();
            else if (compare < 0) {
                long d = entry.getDistinctCount();
                if (d == 0)
                    return 1;
                return simpleRound(entry.getLessCount(), d);
            }
        }
        HistogramEntry lastEntry = entries.get(entries.size() - 1);
        long d = lastEntry.getDistinctCount();
        if (d == 0)
            return 1;
        d++;
        return simpleRound(lastEntry.getLessCount(), d);
    }
    
    protected long rowsBetween(Histogram histogram, 
                               byte[] lowBytes, boolean lowInclusive,
                               byte[] highBytes, boolean highInclusive) {
        boolean before = (lowBytes != null);
        long rowCount = 0;
        byte[] entryStartBytes, entryEndBytes = null;
        for (HistogramEntry entry : histogram.getEntries()) {
            entryStartBytes = entryEndBytes;
            entryEndBytes = entry.getKeyBytes();
            long portionStart = 0;
            if (before) {
                int compare = bytesComparator.compare(lowBytes, entryEndBytes);
                if (compare > 0)
                    continue;
                if (compare == 0) {
                    if (lowInclusive)
                        rowCount += entry.getEqualCount();
                    continue;
                }
                portionStart = uniformPortion(entryStartBytes, entryEndBytes, lowBytes,
                                              entry.getLessCount());
                // Fall through to check high in same entry.
            }
            if (highBytes != null) {
                int compare = bytesComparator.compare(highBytes, entryEndBytes);
                if (compare == 0) {
                    rowCount += entry.getLessCount() - portionStart;
                    if (highInclusive)
                        rowCount += entry.getEqualCount();
                    break;
                }
                if (compare < 0) {
                    rowCount += uniformPortion(entryStartBytes, entryEndBytes, highBytes,
                                               entry.getLessCount()) - portionStart;
                    break;
                }
            }
            rowCount += entry.getLessCount() + entry.getEqualCount() - portionStart;
        }
        return Math.max(rowCount, 1);
    }
    
    /** Assuming that byte strings are uniformly distributed, what
     * would be given position correspond to?
     */
    protected static long uniformPortion(byte[] start, byte[] end, byte[] middle,
                                         long total) {
        int idx = 0;
        while (safeByte(start, idx) == safeByte(end, idx)) idx++; // First mismatch.
        long lstart = 0, lend = 0, lmiddle = 0;
        for (int i = 0; i < 4; i++) {
            lstart = (lstart << 8) + safeByte(start, idx);
            lend = (lend << 8) + safeByte(end, idx);
            lmiddle = (lmiddle << 8) + safeByte(middle, idx);
        }
        return simpleRound((lmiddle - lstart) * total, lend - lstart);
    }

    private static int safeByte(byte[] ba, int idx) {
        if ((ba != null) && (idx < ba.length))
            return ba[idx] & 0xFF;
        else
            return 0;
    }

    protected static long simpleRound(long n, long d) {
        return (n + d / 2) / d;
    }

    /** Encode the given field expressions a comparable key byte array.
     * Or return <code>null</code> if some field is not a constant.
     */
    protected byte[] encodeKeyBytes(Index index, 
                                    List<ExpressionNode> fields,
                                    ExpressionNode anotherField) {
        key.clear();
        keyTarget.attach(key);
        int i = 0;
        if (fields != null) {
            for (ExpressionNode field : fields) {
                if (!encodeKeyValue(field, index, i++)) {
                    return null;
                }
            }
        }
        if (anotherField != null) {
            if (!encodeKeyValue(anotherField, index, i++)) {
                return null;
            }
        }
        byte[] keyBytes = new byte[key.getEncodedSize()];
        System.arraycopy(key.getEncodedBytes(), 0, keyBytes, 0, keyBytes.length);
        return keyBytes;
    }

    protected boolean encodeKeyValue(ExpressionNode node, Index index, int column) {
        Expression expr = null;
        if (node instanceof ConstantExpression) {
            if (node.getAkType() == null)
                expr = Expressions.literal(((ConstantExpression)node).getValue());
            else
                expr = Expressions.literal(((ConstantExpression)node).getValue(),
                                           node.getAkType());
        }
        if (expr == null)
            return false;
        ValueSource valueSource = expr.evaluation().eval();
        keyTarget.expectingType(index.getKeyColumns().get(column).getColumn().getType().akType());
        Converters.convert(valueSource, keyTarget);
        return true;
    }

    /** Estimate the cost of starting at the given table's index and
     * fetching the given tables, then joining them with Flatten and
     * Product. */
    // TODO: Lots of logical overlap with BranchJoiner.
    public CostEstimate costFlatten(TableSource indexTable,
                                    Collection<TableSource> requiredTables) {
        indexTable.getTable().getTree().colorBranches();
        Map<UserTable,FlattenedTable> ftables = new HashMap<UserTable,FlattenedTable>();
        long indexMask = indexTable.getTable().getBranches();
        FlattenedTable iftable = flattened(indexTable, ftables);
        for (TableSource table : orderedTableSources(requiredTables)) {
            FlattenedTable ftable = flattened(table, ftables);
            long common = indexMask & table.getTable().getBranches();
            if (common == indexMask) {
                // The index table or one of its descendants or single branch.
                if (indexTable.getTable().getDepth() >= table.getTable().getDepth()) {
                    ftable.ancestor = true; // Just ancestor until branch needed.
                }
                else {
                    ftable.branchPoint = iftable;
                    iftable.branch = true; // Proper descendant, need whole branch.
                }
            }
            else if (common != 0) {
                // Ancestor
                ftable.ancestor = true;
            }
            else {
                // No common ancestor, need higher branchpoint.
                TableNode tnode = table.getTable();
                do {
                    TableNode prev = tnode;
                    tnode = tnode.getParent();
                    if ((tnode.getBranches() & indexMask) != 0) {
                        ftable.branchPoint = flattened(prev.getTable(), ftables);
                        ftable.branchPoint.branch = true;
                        break;
                    }
                } while (tnode != null);
            }
        }
        // Find single root from which all the flattens descend.
        FlattenedTable root = null;
        for (FlattenedTable ftable : orderedTables(ftables.values())) {
            if ((root == null) || (ftable.table.getDepth() < root.table.getDepth())) {
                root = ftable;
            }
            else if (ftable.table.getDepth() == root.table.getDepth()) {
                // Move up to common ancestor, which is new root.
                while (root != ftable) {
                    root = flattened(root.table.parentTable(), ftables);
                    ftable = flattened(ftable.table.parentTable(), ftables);
                    root.ancestor = ftable.ancestor = true;
                }
            }
        }
        // Limit branchPoint markers to leaves; that's the number that will joined.
        for (FlattenedTable ftable : orderedTables(ftables.values())) {
            if (ftable.branchPoint != null) {
                while (ftable != root) {
                    ftable = flattened(ftable.table.parentTable(), ftables);
                    ftable.branchPoint = null;
                }
            }
        }
        // Account for database accesses.
        long rowCount = 1;
        double cost = 0.0;
        long[] counts = new long[2];
        for (FlattenedTable ftable : orderedTables(ftables.values())) {
            // Multiple rows come in from branches and they all get producted together.
            if (ftable.branchPoint != null)
                rowCount *= branchScale(getTableRowCount(ftable.table), 
                                        ftable.branchPoint, iftable);
            if (ftable.branch) {
                branchRowCount(ftable, iftable, counts);
                cost += RANDOM_ACCESS_COST +
                    SEQUENTIAL_ACCESS_COST * (counts[0] - 1) +
                    FIELD_ACCESS_COST * counts[1];
            }
            else if (ftable.ancestor) {
                cost += RANDOM_ACCESS_COST + FIELD_ACCESS_COST * ftable.table.getColumns().size();
            }
        }
        return new CostEstimate(rowCount, cost);
    }

    // Estimate depends somewhat on which happens to be chosen as the
    // main branch, so make it predictable for tests.
    protected Collection<TableSource> orderedTableSources(Collection<TableSource> tables) {
        List<TableSource> ordered = new ArrayList<TableSource>(tables);
        Collections.sort(ordered, new Comparator<TableSource>() {
                             @Override
                             public int compare(TableSource t1, TableSource t2) {
                                 return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
                             }
                         });
        return ordered;
    }

    protected Collection<FlattenedTable> orderedTables(Collection<FlattenedTable> tables) {
        List<FlattenedTable> ordered = new ArrayList<FlattenedTable>(tables);
        Collections.sort(ordered, new Comparator<FlattenedTable>() {
                             @Override
                             public int compare(FlattenedTable f1, FlattenedTable f2) {
                                 return f1.table.getTableId().compareTo(f2.table.getTableId());
                             }
                         });
        return ordered;
    }

    // A source of flattened rows.
    static class FlattenedTable {
        UserTable table;
        boolean ancestor, branch;
        FlattenedTable branchPoint;
        
        public FlattenedTable(UserTable table) {
            this.table = table;
        }

        @Override
        public String toString() {
            return table.getName().toString();
        }
    }

    protected FlattenedTable flattened(UserTable table, 
                                       Map<UserTable,FlattenedTable> map) {
        FlattenedTable ftable = map.get(table);
        if (ftable == null) {
            ftable = new FlattenedTable(table);
            map.put(table, ftable);
        }
        return ftable;
    }

    protected FlattenedTable flattened(TableSource table, 
                                       Map<UserTable,FlattenedTable> map) {
        return flattened(table.getTable().getTable(), map);
    }

    // Number of rows that come in from a BranchLookup starting at
    // this table, including ones that aren't even known to the
    // optimizer / operator plan and will just get ignored. They still
    // stream through.
    protected void branchRowCount(FlattenedTable ftable, FlattenedTable indexTable,
                                  long[] counts) {
        for (int i = 0; i < counts.length; i++) {
            counts[i] = 0;
        }
        totalCardinalities(ftable.table, counts);
        for (int i = 0; i < counts.length; i++) {
            counts[i] = branchScale(counts[i], ftable, indexTable);
        }
    }
    
    // Branching from the index just has one branchpoint
    // row. Branching from someplace else has as many as there are per
    // the common ancestor, which is the immediate parent of the
    // branchpoint.
    protected long branchScale(long total, 
                               FlattenedTable ftable, FlattenedTable indexTable) {
        return simpleRound(total,
                           getTableRowCount((ftable == indexTable) ?
                                            ftable.table :
                                            ftable.table.parentTable()));
    }

    protected void totalCardinalities(UserTable table, long[] counts) {
        long total = getTableRowCount(table);
        counts[0] += total;
        counts[1] += total * table.getColumns().size();
        for (Join childJoin : table.getChildJoins()) {
            totalCardinalities(childJoin.getChild(), counts);
        }
    }

    /** Estimate the cost of testing some conditions. */
    // TODO: Could estimate result cardinality based on (easily
    // determinable) selectivities.
    public CostEstimate costSelect(Collection<ConditionExpression> conditions,
                                   long size) {
        return new CostEstimate(size, conditions.size() * SELECT_COST);
    }

    /** Estimate the cost of a sort of the given size. */
    public CostEstimate costSort(long size) {
        return new CostEstimate(size, size * Math.log(size) * SORT_COST);
    }

    /** Estimate cost of scanning the whole group. */
    // TODO: Need to account for tables actually wanted?
    public CostEstimate costGroupScan(Group group) {
        long nrows = 0;
        double cost = RANDOM_ACCESS_COST;
        for (UserTable table : group.getGroupTable().getAIS().getUserTables().values()) {
            if (table.getGroup() == group) {
                long rowCount = getTableRowCount(table);
                nrows += rowCount;
                cost += rowCount * (SEQUENTIAL_ACCESS_COST + FIELD_ACCESS_COST * table.getColumns().size());
            }
        }
        return new CostEstimate(nrows, cost);
    }

}
