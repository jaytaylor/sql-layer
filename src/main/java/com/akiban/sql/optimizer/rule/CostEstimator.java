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

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
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

    // TODO: Temporary until fully installed.
    public boolean isEnabled() {
        return false;
    }

    // TODO: These need to be figured out for real.
    public static final double RANDOM_ACCESS_COST = 5.0;
    public static final double SEQUENTIAL_ACCESS_COST = 1.0;

    /** Estimate cost of scanning from this index. */
    public CostEstimate costIndexScan(Index index,
                                      List<ExpressionNode> equalityComparands,
                                      ExpressionNode lowComparand, boolean lowInclusive,
                                      ExpressionNode highComparand, boolean highInclusive) {
        if (index.isUnique()) {
            if ((equalityComparands != null) &&
                (equalityComparands.size() == index.getColumns().size())) {
                // Exact match from unique index; probably one row.
                return new CostEstimate(1, RANDOM_ACCESS_COST);
            }
        }
        IndexStatistics indexStats = getIndexStatistics(index);
        long rowCount = getTableRowCount(index.leafMostTable());
        int columnCount = 0;
        if (equalityComparands != null)
            columnCount = equalityComparands.size();
        if ((lowComparand != null) || (highComparand != null))
            columnCount++;
        Histogram histogram;
        if ((indexStats == null) ||
            (indexStats.getRowCount() == 0) ||
            (columnCount == 0) ||
            ((histogram = indexStats.getHistogram(columnCount)) == null)) {
            // No stats or just used for ordering.
            // TODO: Is this too conservative?
            return new CostEstimate(rowCount, 
                                    RANDOM_ACCESS_COST + (rowCount * SEQUENTIAL_ACCESS_COST));
        }
        long nrows;
        if ((lowComparand == null) && (highComparand == null)) {
            // Equality lookup.
            byte[] keyBytes = encodeKeyBytes(index, equalityComparands, null);
            if (keyBytes == null) {
                // Variable.
                nrows = indexStats.getRowCount() / histogram.totalDistinctCount();
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
        if (true)
            nrows = (nrows * rowCount) / indexStats.getRowCount();
        return new CostEstimate(nrows, 
                                RANDOM_ACCESS_COST + (nrows * SEQUENTIAL_ACCESS_COST));
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
        keyTarget.expectingType(index.getColumns().get(column).getColumn().getType().akType());
        Converters.convert(valueSource, keyTarget);
        return true;
    }

    protected long rowsEqual(Histogram histogram, byte[] keyBytes) {
        // TODO; Could use Collections.binarySearch if we had
        // something that looked like a HistogramEntry.
        List<HistogramEntry> entries = histogram.getEntries();
        int i = 0;
        while (i < entries.size()) {
            HistogramEntry entry = entries.get(i);
            int compare = bytesComparator.compare(keyBytes, entry.getKeyBytes());
            if (compare == 0)
                return entry.getEqualCount();
        }
        return 0;
    }

    protected long rowsBetween(Histogram histogram, 
                               byte[] lowBytes, boolean lowInclusive,
                               byte[] highBytes, boolean highInclusive) {
        return 0;
    }
    
    /** Estimate the cost of starting at the given table's index and
     * fetching the given tables, then joining them with Flatten and
     * Product. */
    // TODO: Lots of overlap with BranchJoiner. Once group joins are
    // picked through the same join enumeration of other kinds, this
    // should be better integrated.
    public CostEstimate costFlatten(TableSource indexTable,
                                    Collection<TableSource> requiredTables,
                                    long repeat) {
        int nbranches = indexTable.getTable().getTree().colorBranches();
        return new CostEstimate(0, 0);
    }

    /** Estimate the cost of a sort of the given size. */
    public CostEstimate costSort(long size) {
        return new CostEstimate(0, 0);
    }
}
