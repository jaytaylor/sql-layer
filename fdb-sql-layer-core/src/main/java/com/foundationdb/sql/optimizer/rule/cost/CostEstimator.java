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

package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.store.statistics.Histogram;
import com.foundationdb.server.store.statistics.HistogramEntry;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.optimizer.rule.SchemaRulesContext;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.statistics.IndexStatistics;
import com.foundationdb.server.types.TInstance;
import com.persistit.Key;

import com.google.common.primitives.UnsignedBytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import static java.lang.Math.round;

public abstract class CostEstimator implements TableRowCounts
{
    private static final Logger logger = LoggerFactory.getLogger(CostEstimator.class);

    private final Schema schema;
    private final Properties properties;
    private final CostModel model;
    private final Key key;
    private final PersistitKeyValueTarget keyPTarget;
    private final Comparator<byte[]> bytesComparator;

    protected CostEstimator(Schema schema, Properties properties, KeyCreator keyCreator,
                            CostModelFactory modelFactory) {
        this.schema = schema;
        this.properties = properties;
        model = modelFactory.newCostModel(schema, this);
        key = keyCreator.createKey();
        keyPTarget = new PersistitKeyValueTarget(getClass().getSimpleName());
        bytesComparator = UnsignedBytes.lexicographicalComparator();
    }

    protected CostEstimator(SchemaRulesContext rulesContext, KeyCreator keyCreator,
                            CostModelFactory modelFactory) {
        this(rulesContext.getSchema(), rulesContext.getProperties(), 
             keyCreator, modelFactory);
    }

    public CostModel getCostModel() {
        return model;
    }

    protected CostEstimate adjustCostEstimate(CostEstimate costEstimate) {
        return model.adjustCostEstimate(costEstimate);
    }

    @Override
    public long getTableRowCount(Table table) {
        long count = getTableRowCountFromStatistics(table);
        if (count >= 0)
            return count;
        return 1;
    }

    protected long getTableRowCountFromStatistics(Table table) {
        // This implementation is only for testing; normally overridden by real server.
        // Return row count (not sample count) from analysis time.
        for (Index index : table.getIndexes()) {
            IndexStatistics istats = getIndexStatistics(index);
            if (istats != null)
                return istats.getRowCount();
        }
        return -1;              // Not analyzed.
    }

    public abstract IndexStatistics getIndexStatistics(Index index);

    public void getIndexColumnStatistics(Index index, Index[] indexColumnsIndexes, Histogram[] histograms) {
        List<IndexColumn> allIndexColumns = index.getAllColumns();
        IndexStatistics statsForRequestedIndex = getIndexStatistics(index);
        int nIndexColumns = allIndexColumns.size();
        int nKeyColumns = index.getKeyColumns().size();
        for (int i = 0; i < nIndexColumns; i++) {
            Histogram histogram = null;
            Index indexColumnsIndex = null;
            // Use a histogram of the index itself, if possible.
            if (i < nKeyColumns && statsForRequestedIndex != null) {
                indexColumnsIndex = index;
                histogram = statsForRequestedIndex.getHistogram(i, 1);
            }
            if (histogram == null) {
                indexColumnsIndex = (i == 0) ? index : null;
                // If none, find a TableIndex whose first column is leadingColumn
                IndexStatistics indexStatistics = null;
                Column leadingColumn = allIndexColumns.get(i).getColumn();
                for (TableIndex tableIndex : leadingColumn.getTable().getIndexes()) {
                    if (tableIndex.getKeyColumns().get(0).getColumn() == leadingColumn) {
                        indexStatistics = getIndexStatistics(tableIndex);
                        if (indexStatistics != null) {
                            indexColumnsIndex = tableIndex;
                            histogram = indexStatistics.getHistogram(0, 1);
                            break;
                        }
                        else if (indexColumnsIndex == null) {
                            indexColumnsIndex = tableIndex;
                        }
                    }
                }
                // If none, find a GroupIndex whose first column is leadingColumn
                if (indexStatistics == null) {
                    groupLoop: for (Group group : schema.ais().getGroups().values()) {
                        for (GroupIndex groupIndex : group.getIndexes()) {
                            if (groupIndex.getKeyColumns().get(0).getColumn() == leadingColumn) {
                                indexStatistics = getIndexStatistics(groupIndex);
                                if (indexStatistics != null) {
                                    indexColumnsIndex = groupIndex;
                                    histogram = indexStatistics.getHistogram(0, 1);
                                    break groupLoop;
                                }
                                else if (indexColumnsIndex == null) {
                                    indexColumnsIndex = groupIndex;
                                }
                            }
                        }
                    }
                }
            }
            indexColumnsIndexes[i] = indexColumnsIndex;
            histograms[i] = histogram;
        }
    }

    /* Settings.
     * Note: these are compiler properties, so they start with
     * optimizer.cost. in the server.properties file. 
     */

    protected final double DEFAULT_MISSING_STATS_SELECTIVITY = 0.85;

    protected double missingStatsSelectivity() {
        String str = getProperty("cost.missingStatsSelectivity");
        if (str != null)
            return Double.valueOf(str);
        else
            return DEFAULT_MISSING_STATS_SELECTIVITY;
    }

    protected String getProperty(String key) {
        return properties.getProperty(key);
    }

    protected String getProperty(String key, String defval) {
        return properties.getProperty(key, defval);
    }

    /** Estimate cost of scanning from this index. */
    public CostEstimate costIndexScan(Index index,
                                      List<ExpressionNode> equalityComparands,
                                      ExpressionNode lowComparand, boolean lowInclusive,
                                      ExpressionNode highComparand, boolean highInclusive) {
        return costIndexScan(index, sizeIndexScan(index, equalityComparands,
                                                  lowComparand, lowInclusive,
                                                  highComparand, highInclusive));
    }

    /** Estimate number of rows returned from this index. */
    public long sizeIndexScan(Index index,
                              List<ExpressionNode> equalityComparands,
                              ExpressionNode lowComparand, boolean lowInclusive,
                              ExpressionNode highComparand, boolean highInclusive) {
        if (index.isUnique()) {
            if ((equalityComparands != null) &&
                (equalityComparands.size() >= index.getKeyColumns().size())) {
                // Exact match from unique index; probably one row.
                return 1;
            }
        }
        Table indexedTable = index.leafMostTable();
        long rowCount = getTableRowCount(indexedTable);
        // TODO: FIX THIS COMMENT. Should it refer to getSingleColumnHistogram?
        // Get IndexStatistics for each column. If the ith element is non-null, then it definitely has
        // a leading-column histogram (obtained by IndexStats.getHistogram(1)).
        // else: There are no index stats for the first column of the index. Either there is no such index,
        // or there is, but it doesn't have stats.
        int nidxcols = index.getAllColumns().size();
        Index[] indexColumnsIndexes = new Index[nidxcols];
        Histogram[] histograms = new Histogram[nidxcols];
        getIndexColumnStatistics(index, indexColumnsIndexes, histograms);
        int columnCount = 0;
        if (equalityComparands != null)
            columnCount = equalityComparands.size();
        if ((lowComparand != null) || (highComparand != null))
            columnCount++;
        if (columnCount == 0) {
            // Index just used for ordering.
            return rowCount;
        }
        boolean scaleCount = true;
        double selectivity = 1.0;
        if (equalityComparands != null && !equalityComparands.isEmpty()) {
            selectivity = fractionEqual(index,
                                        indexColumnsIndexes,
                                        histograms,
                                        equalityComparands);
        }
        if (lowComparand != null || highComparand != null) {
            selectivity *= fractionBetween(index.getAllColumns().get(columnCount - 1).getColumn(),
                                           indexColumnsIndexes[columnCount - 1],
                                           histograms[columnCount - 1],
                                           lowComparand, lowInclusive,
                                           highComparand, highInclusive);
        }
        if (mostlyDistinct(histograms)) scaleCount = false;
        // statsCount: Number of rows in the table based on an index of the table, according to index
        //    statistics, which may be stale.
        // rowCount: Approximate number of rows in the table, reasonably up to date.
        long statsCount;
        IndexStatistics stats = tableIndexStatistics(indexedTable, indexColumnsIndexes, histograms);
        if (stats != null) {
            statsCount = stats.getSampledCount();
        }
        else {
            statsCount = rowCount;
            scaleCount = false;
        }
        long nrows = Math.max(1, round(selectivity * statsCount));
        if (scaleCount) {
            checkRowCountChanged(indexedTable, stats, rowCount);
            if ((rowCount > 0) && (statsCount > 0))
                nrows = simpleRound((nrows * rowCount), statsCount);
        }
        return nrows;
    }

    private IndexStatistics tableIndexStatistics(Table indexedTable,
                                                 Index[] indexColumnsIndexes,
                                                 Histogram[] histograms) {
        // At least one of the index columns must be from the indexed table
        for (int i = 0; i < indexColumnsIndexes.length; i++) {
            Index index = indexColumnsIndexes[i];
            if (index != null) {
                Column leadingColumn = index.getKeyColumns().get(0).getColumn();
                if (leadingColumn.getTable() == indexedTable && histograms[i] != null) {
                    return histograms[i].getIndexStatistics();
                }
            }
        }
        // No index stats available.
        return null;
    }
    
    /** Estimate cost of scanning given number of rows from this index. 
     * One random access to get there, then nrows-1 sequential accesses following,
     * Plus a surcharge for copying something as wide as the index.
     */
    public CostEstimate costIndexScan(Index index, long nrows) {
        return new CostEstimate(nrows, 
                                model.indexScan(schema.indexRowType(index), (int)nrows));
    }

    protected double fractionEqual(Index index, 
                                   Index[] indexColumnsIndexes,
                                   Histogram[] histograms,
                                   List<ExpressionNode> eqExpressions) {
        double selectivity = 1.0;
        keyPTarget.attach(key);
        for (int column = 0; column < eqExpressions.size(); column++) {
            ExpressionNode node = eqExpressions.get(column);
            Histogram histogram = histograms[column];
            selectivity *= fractionEqual(index.getAllColumns().get(column).getColumn(),
                                         indexColumnsIndexes[column],
                                         histogram,
                                         node);
        }
        return selectivity;
    }

    protected double fractionEqual(Column column, 
                                   Index index,
                                   Histogram histogram,
                                   ExpressionNode expr) {
        if (histogram == null) {
            missingStats(index, column);
            return missingStatsSelectivity();
        } else {
            long indexStatsSampledCount = histogram.getIndexStatistics().getSampledCount();
            if (histogram.getEntries().isEmpty()) {
                missingStats(index, column);
                return missingStatsSelectivity();
            } else if ((expr instanceof ColumnExpression) &&
                       (((ColumnExpression)expr).getTable() instanceof ExpressionsSource)) {
                // Can do better than unknown if we know some actual values.
                // Compute the average selectivity among them.
                ColumnExpression toColumn = (ColumnExpression)expr;
                ExpressionsSource values = (ExpressionsSource)toColumn.getTable();
                int position = toColumn.getPosition();
                double sum = 0.0;
                int count = 0;
                for (List<ExpressionNode> row : values.getExpressions()) {
                    sum += fractionEqual(column, index, histogram, row.get(position));
                    count++;
                }
                if (count > 0) sum /= count;
                return sum;
            } else {
                key.clear();
                keyPTarget.attach(key);
                // encodeKeyValue evaluates non-null iff node is a constant expression. key is initialized as a side-effect.
                byte[] columnValue = encodeKeyValue(expr, index, histogram.getFirstColumn()) ? keyCopy() : null;
                if (columnValue == null) {
                    // Variable expression. Use average selectivity for histogram.
                    return
                        mostlyDistinct(histogram)
                        ? 1.0 / indexStatsSampledCount
                        : 1.0 / histogram.totalDistinctCount();
                } else {
                    // TODO: Could use Collections.binarySearch if we had something that looked like a HistogramEntry.
                    List<HistogramEntry> entries = histogram.getEntries();
                    for (HistogramEntry entry : entries) {
                        // Constant expression
                        int compare = bytesComparator.compare(columnValue, entry.getKeyBytes());
                        if (compare == 0) {
                            return ((double) entry.getEqualCount()) / indexStatsSampledCount;
                        } else if (compare < 0) {
                            long d = entry.getDistinctCount();
                            return d == 0 ? 0.0 : ((double) entry.getLessCount()) / (d * indexStatsSampledCount);
                        }
                    }
                    HistogramEntry lastEntry = entries.get(entries.size() - 1);
                    long d = lastEntry.getDistinctCount();
                    if (d == 0) {
                        return 1;
                    }
                    return 0.00483;
                }
            }
        }
    }

    protected double fractionBetween(Column column,
                                     Index index,
                                     Histogram histogram,
                                     ExpressionNode lo, boolean lowInclusive,
                                     ExpressionNode hi, boolean highInclusive)
    {
        if (histogram == null || histogram.getEntries().isEmpty()) {
            missingStats(index, column);
            return missingStatsSelectivity();
        }
        keyPTarget.attach(key);
        key.clear();
        byte[] loBytes = encodeKeyValue(lo, index, histogram.getFirstColumn()) ? keyCopy() : null;
        key.clear();
        byte[] hiBytes = encodeKeyValue(hi, index, histogram.getFirstColumn()) ? keyCopy() : null;
        if (loBytes == null && hiBytes == null) {
            return missingStatsSelectivity();
        }
        boolean before = (loBytes != null);
        long rowCount = 0;
        byte[] entryStartBytes, entryEndBytes = null;
        for (HistogramEntry entry : histogram.getEntries()) {
            entryStartBytes = entryEndBytes;
            entryEndBytes = entry.getKeyBytes();
            long portionStart = 0;
            if (before) {
                int compare = bytesComparator.compare(loBytes, entryEndBytes);
                if (compare > 0) {
                    continue;
                }
                if (compare == 0) {
                    if (lowInclusive) {
                        rowCount += entry.getEqualCount();
                    }
                    continue;
                }
                portionStart = uniformPortion(entryStartBytes, 
                                              entryEndBytes, 
                                              loBytes, 
                                              entry.getLessCount());
                before = false; // Don't include uniformPortion for subsequent buckets.
                // Fall through to check high in same entry.
            }
            if (hiBytes != null) {
                int compare = bytesComparator.compare(hiBytes, entryEndBytes);
                if (compare == 0) {
                    rowCount += entry.getLessCount() - portionStart;
                    if (highInclusive) {
                        rowCount += entry.getEqualCount();
                    }
                    break;
                }
                if (compare < 0) {
                    rowCount += uniformPortion(entryStartBytes, 
                                               entryEndBytes, 
                                               hiBytes,
                                               entry.getLessCount()) - portionStart;
                    break;
                }
            }
            rowCount += entry.getLessCount() + entry.getEqualCount() - portionStart;
        }
        return ((double) Math.max(rowCount, 1)) / histogram.getIndexStatistics().getSampledCount();
    }


    // Must be provably mostly distinct: Every histogram is available and mostly distinct.
    private boolean mostlyDistinct(Histogram[] histograms)
    {
        for (Histogram histogram : histograms) {
            if (histogram == null) {
                return false;
            } else {
                if (!mostlyDistinct(histogram)) {
                    // < 90% distinct
                    return false;
                }
            }
        }
        return true;
    }

    private boolean mostlyDistinct(Histogram histogram)
    {
        return
            histogram != null &&
            histogram.totalDistinctCount() * 10 > histogram.getIndexStatistics().getSampledCount() * 9;
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
            lstart = (lstart << 8) + safeByte(start, idx+i);
            lend = (lend << 8) + safeByte(end, idx+i);
            lmiddle = (lmiddle << 8) + safeByte(middle, idx+i);
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
                                    ExpressionNode anotherField,
                                    boolean upper) {
        key.clear();
        keyPTarget.attach(key);
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
        else if (upper) {
            key.append(Key.AFTER);
        }
        return keyCopy();
    }    
    
    protected boolean isConstant(ExpressionNode node)
    {
        return node.isConstant();
    }

    protected boolean encodeKeyValue(ExpressionNode node, Index index, int column) {
        ValueSource value = null;
        if (node instanceof ConstantExpression) {
            if (node.getPreptimeValue() != null) {
                if (node.getType() == null) { // Literal null
                    keyPTarget.putNull();
                    return true;
                }
                value = node.getPreptimeValue().value();
            }
        }
        else if (node instanceof ParameterExpression && ((ParameterExpression)node).isSet()) {
            if (((ParameterExpression)node).getValue() == null) {
                keyPTarget.putNull();
                return true;
            }
            value = ValueSources.fromObject(((ParameterExpression)node).getValue(),
                    node.getPreptimeValue().type()).value();
        }
        else if (node instanceof IsNullIndexKey) {
            keyPTarget.putNull();
            return true;
        }
        if (value == null)
            return false;
        TInstance type;
        determine_type:
        {
            if (index.isSpatial()) {
                int firstSpatialColumn = index.firstSpatialArgument();
                if (column == firstSpatialColumn) {
                    type = InternalIndexTypes.LONG.instance(node.getPreptimeValue().isNullable());
                    break determine_type;
                }
                else if (column > firstSpatialColumn) {
                    column += index.spatialColumns() - 1;
                }
            }
            type = index.getAllColumns().get(column).getColumn().getType();
        }
        type.writeCollating(value, keyPTarget);
        return true;
    }

    private byte[] keyCopy()
    {
        byte[] keyBytes = new byte[key.getEncodedSize()];
        System.arraycopy(key.getEncodedBytes(), 0, keyBytes, 0, keyBytes.length);
        return keyBytes;
    }

    /** Estimate the cost of intersecting a left-deep multi-index intersection. */
    public CostEstimate costIndexIntersection(MultiIndexIntersectScan intersection, IndexIntersectionCoster coster)
    {
        IntersectionCostRunner runner = new IntersectionCostRunner(coster);
        runner.buildIndexScanCost(intersection);
        CostEstimate estimate = new CostEstimate(runner.rowCount, runner.cost);
        return estimate;
    }

    /** Estimate the cost of starting at the given table's index and
     * fetching the given tables, then joining them with Flatten and
     * Product. */
    public CostEstimate costFlatten(TableGroupJoinTree tableGroup,
                                    TableSource indexTable,
                                    Set<TableSource> requiredTables) {
        TableGroupJoinNode startNode = tableGroup.getRoot().findTable(indexTable);
        coverBranches(tableGroup, startNode, requiredTables);
        long rowCount = 1;
        double cost = 0.0;
        List<TableRowType> ancestorTypes = new ArrayList<>();
        for (TableGroupJoinNode ancestorNode = startNode;
             ancestorNode != null;
             ancestorNode = ancestorNode.getParent()) {
            if (isRequired(ancestorNode)) {
                if ((ancestorNode == startNode) &&
                    (getSideBranches(ancestorNode) != 0)) {
                    continue;   // Branch, not ancestor.
                }
                ancestorTypes.add(schema.tableRowType(ancestorNode.getTable().getTable().getTable()));
            }
        }
        // Cost to get main branch.
        cost += model.ancestorLookup(ancestorTypes);
        for (TableGroupJoinNode branchNode : tableGroup) {
            if (isSideBranchLeaf(branchNode)) {
                int branch = Long.numberOfTrailingZeros(getBranches(branchNode));
                TableGroupJoinNode branchRoot = branchNode, nextToRoot = null;
                while (true) {
                    TableGroupJoinNode parent = branchRoot.getParent();
                    if (parent == startNode) {
                        // Different kind of BranchLookup.
                        nextToRoot = branchRoot = parent;
                        break;
                    }
                    if ((parent == null) || !onBranch(parent, branch))
                        break;
                    nextToRoot = branchRoot;
                    branchRoot = parent;
                }
                assert (nextToRoot != null);
                // Multiplier from this branch.
                rowCount *= descendantCardinality(branchNode, branchRoot);
                // Cost to get side branch.
                cost += model.branchLookup(schema.tableRowType(nextToRoot.getTable().getTable().getTable()));
            }
        }
        for (TableGroupJoinNode node : tableGroup) {
            if (isFlattenable(node)) {
                long nrows = tableCardinality(node);
                // Cost of flattening these children with their ancestor.
                cost += model.flatten((int)nrows);
            }
        }
        if (rowCount > 1)
            cost += model.product((int)rowCount);
        return new CostEstimate(rowCount, cost);
    }

    /** Estimate the cost of starting from a group scan and joining
     * with Flatten and Product. */
    public CostEstimate costFlattenGroup(TableGroupJoinTree tableGroup,
                                         Set<TableSource> requiredTables) {
        TableGroupJoinNode rootNode = tableGroup.getRoot();
        coverBranches(tableGroup, rootNode, requiredTables);
        int branchCount = 0;
        long rowCount = 1;
        double cost = 0.0;
        for (TableGroupJoinNode node : tableGroup) {
            if (isFlattenable(node)) {
                long nrows = getTableRowCount(node.getTable().getTable().getTable());
                // Cost of flattening these children with their ancestor.
                cost += model.flatten((int)nrows);
                if (isSideBranchLeaf(node)) {
                    // Leaf of a new branch.
                    branchCount++;
                    rowCount *= nrows;
                }
            }
        }
        if (branchCount > 1)
            cost += model.product((int)rowCount);
        return new CostEstimate(rowCount, cost);
    }

    /** Estimate the cost of getting the desired number of flattened
     * rows from a group scan. This combined costing of the partial
     * scan itself and the flatten, since they are tied together. */
    public CostEstimate costPartialGroupScanAndFlatten(TableGroupJoinTree tableGroup,
                                                       Set<TableSource> requiredTables,
                                                       Map<Table,Long> tableCounts) {
        TableGroupJoinNode rootNode = tableGroup.getRoot();
        coverBranches(tableGroup, rootNode, requiredTables);
        int branchCount = 0;
        long rowCount = 1;
        double cost = 0.0;
        for (Map.Entry<Table,Long> entry : tableCounts.entrySet()) {
            cost += model.partialGroupScan(schema.tableRowType(entry.getKey()),
                                           entry.getValue());
        }
        for (TableGroupJoinNode node : tableGroup) {
            if (isFlattenable(node)) {
                long nrows = tableCounts.get(node.getTable().getTable().getTable());
                // Cost of flattening these children with their ancestor.
                cost += model.flatten((int)nrows);
                if (isSideBranchLeaf(node)) {
                    // Leaf of a new branch.
                    branchCount++;
                    rowCount *= nrows;
                }
            }
        }
        if (branchCount > 1)
            cost += model.product((int)rowCount);
        return new CostEstimate(rowCount, cost);
    }

    /** Estimate the cost of starting from outside the loop in the same group. */
    public CostEstimate costFlattenNested(TableGroupJoinTree tableGroup,
                                          TableSource outsideTable,
                                          TableSource insideTable,
                                          boolean insideIsParent,
                                          Set<TableSource> requiredTables) {
        TableGroupJoinNode startNode = tableGroup.getRoot().findTable(insideTable);
        coverBranches(tableGroup, startNode, requiredTables);
        int branchCount = 0;
        long rowCount = 1;
        double cost = 0.0;
        if (insideIsParent) {
            cost += model.ancestorLookup(Collections.singletonList(schema.tableRowType(insideTable.getTable().getTable())));
        }
        else {
            rowCount *= descendantCardinality(insideTable, outsideTable);
            cost += model.branchLookup(schema.tableRowType(insideTable.getTable().getTable()));
        }
        for (TableGroupJoinNode node : tableGroup) {
            if (isFlattenable(node)) {
                long nrows = tableCardinality(node);
                // Cost of flattening these children with their ancestor.
                cost += model.flatten((int)nrows);
                if (isSideBranchLeaf(node)) {
                    // Leaf of a new branch.
                    branchCount++;
                    rowCount *= nrows;
                }
            }
        }
        if (branchCount > 1)
            cost += model.product((int)rowCount);
        return new CostEstimate(rowCount, cost);
    }

    /** This table needs to be included in flattens. */
    protected static final long REQUIRED = 1;
    /** This table is on the main branch. */
    protected static final long ANCESTOR = 2;
    protected static final int ANCESTOR_BRANCH = 1;
    /** Mask for main or side branch. */
    protected static final long BRANCH_MASK = ~1;
    /** Mask for side branch. */
    protected static final long SIDE_BRANCH_MASK = ~3;

    protected static boolean isRequired(TableGroupJoinNode table) {
        return ((table.getState() & REQUIRED) != 0);
    }
    protected static void setRequired(TableGroupJoinNode table) {
        table.setState(table.getState() | REQUIRED);
    }
    protected static boolean isAncestor(TableGroupJoinNode table) {
        return ((table.getState() & ANCESTOR) != 0);
    }
    protected static long getBranches(TableGroupJoinNode table) {
        return (table.getState() & BRANCH_MASK);
    }
    protected static long getSideBranches(TableGroupJoinNode table) {
        return (table.getState() & SIDE_BRANCH_MASK);
    }
    protected static boolean onBranch(TableGroupJoinNode table, int b) {
        return ((table.getState() & (1 << b)) != 0);
    }
    protected void setBranch(TableGroupJoinNode table, int b) {
        table.setState(table.getState() | (1 << b));
    }

    /** Like {@link BranchJoiner#markBranches} but simpler without
     * having to worry about the exact <em>order</em> in which
     * operations are performed.
     */
    protected void coverBranches(TableGroupJoinTree tableGroup, 
                                 TableGroupJoinNode startNode,
                                 Set<TableSource> requiredTables) {
        for (TableGroupJoinNode table : tableGroup) {
            table.setState(requiredTables.contains(table.getTable()) ? REQUIRED : 0);
        }
        int nbranches = ANCESTOR_BRANCH;
        boolean anyAncestorRequired = false;
        for (TableGroupJoinNode table = startNode; table != null; table = table.getParent()) {
            setBranch(table, nbranches);
            if (isRequired(table))
                anyAncestorRequired = true;
        }
        nbranches++;
        for (TableGroupJoinNode table : tableGroup) {
            if (isSideBranchLeaf(table)) {
                // This is the leaf of a new side branch.
                while (true) {
                    boolean onBranchAlready = (getBranches(table) != 0);
                    setBranch(table, nbranches);
                    if (onBranchAlready) {
                        if (!isRequired(table)) {
                            // Might become required for joining of branches.
                            if (Long.bitCount(anyAncestorRequired ?
                                              getBranches(table) :
                                              getSideBranches(table)) > 1)
                                setRequired(table);
                        }
                        break;
                    }
                    table = table.getParent();
                }
                nbranches++;
            }
        }
    }
    
    /** A table is the leaf of some side branch if it's required but
     * none of its descendants are. */
    protected boolean isSideBranchLeaf(TableGroupJoinNode table) {
        if (!isRequired(table) || isAncestor(table))
            return false;
        for (TableGroupJoinNode descendant : table) {
            if ((descendant != table) && isRequired(descendant))
                return false;
        }
        return true;
    }

    /** A table is flattened in if it's required and one of its
     * ancestors is as well. */
    protected boolean isFlattenable(TableGroupJoinNode table) {
        if (!isRequired(table))
            return false;
        while (true) {
            table = table.getParent();
            if (table == null) break;
            if (isRequired(table))
                return true;
        }
        return false;
    }

    /** Number of rows of given table, total per index row. */
    protected long tableCardinality(TableGroupJoinNode table) {
        if (isAncestor(table))
            return 1;
        TableGroupJoinNode parent = table;
        while (true) {
            parent = parent.getParent();
            if (isAncestor(parent))
                return descendantCardinality(table, parent);
        }
    }

    /** Number of child rows per ancestor. 
     * Never returns zero to avoid contaminating product estimate.
     */
    protected long descendantCardinality(TableGroupJoinNode childNode, 
                                         TableGroupJoinNode ancestorNode) {
        return descendantCardinality(childNode.getTable(), ancestorNode.getTable());
    }

    protected long descendantCardinality(TableSource childTable, 
                                         TableSource ancestorTable) {
        long childCount = getTableRowCount(childTable.getTable().getTable());
        long ancestorCount = getTableRowCount(ancestorTable.getTable().getTable());
        if (ancestorCount == 0) return 1;
        return Math.max(simpleRound(childCount, ancestorCount), 1);
    }

    /** Estimate the cost of testing some conditions. */
    // TODO: Assumes that each condition turns into a separate select.
    public CostEstimate costSelect(Collection<ConditionExpression> conditions,
                                   double selectivity,
                                   long size) {
        int nconds = 0;         // Approximate number of predicate tests.
        for (ConditionExpression cond : conditions) {
            if (cond instanceof InListCondition)
                nconds += ((InListCondition)cond).getExpressions().size();
            // TODO: Maybe various kinds of subquery predicate get high count?
            else
                nconds++;
        }
        return new CostEstimate(Math.max(1, round(size * selectivity)),
                                model.select((int)size) * nconds);
    }

    public CostEstimate costSelect(Collection<ConditionExpression> conditions,
                                   SelectivityConditions selectivityConditions,
                                   long size) {
        return costSelect(conditions, conditionsSelectivity(selectivityConditions), size);
    }

    public static class SelectivityConditions {
        private Map<ColumnExpression,Collection<ConditionExpression>> map =
            new HashMap<>();
        
        public void addCondition(ColumnExpression column, ConditionExpression condition) {
            Collection<ConditionExpression> entry = map.get(column);
            if (entry == null) {
                entry = new ArrayList<>();
                map.put(column, entry);
            }
            entry.add(condition);
        }

        public Iterable<ColumnExpression> getColumns() {
            return map.keySet();
        }

        public Collection<ConditionExpression> getConditions(ColumnExpression column) {
            return map.get(column);
        }
    }

    public double conditionsSelectivity(SelectivityConditions conditions) {
        double selectivity = 1.0;
        for (ColumnExpression entry : conditions.getColumns()) {
            Index index = null;
            IndexStatistics indexStatistics = null;
            Column column = entry.getColumn();
            // Find a TableIndex whose first column is leadingColumn
            for (TableIndex tableIndex : column.getTable().getIndexes()) {
                if (!tableIndex.isSpatial() && tableIndex.getKeyColumns().get(0).getColumn() == column) {
                    indexStatistics = getIndexStatistics(tableIndex);
                    if (indexStatistics != null) {
                        index = tableIndex;
                        break;
                    }
                }
            }
            // If none, find a GroupIndex whose first column is leadingColumn
            if (indexStatistics == null) {
                groupLoop: for (Group group : schema.ais().getGroups().values()) {
                    for (GroupIndex groupIndex : group.getIndexes()) {
                        if (!groupIndex.isSpatial() && groupIndex.getKeyColumns().get(0).getColumn() == column) {
                            indexStatistics = getIndexStatistics(groupIndex);
                            if (indexStatistics != null) {
                                index = groupIndex;
                                break groupLoop;
                            }
                        }
                    }
                }
            }
            if (indexStatistics == null) continue;
            ExpressionNode eq = null, ne = null, lo = null, hi = null;
            boolean loInc = false, hiInc = false;
            List<ExpressionNode> in = null;
            for (ConditionExpression cond : conditions.getConditions(entry)) {
                if (cond instanceof ComparisonCondition) {
                    ComparisonCondition ccond = (ComparisonCondition)cond;
                    switch (ccond.getOperation()) {
                    case EQ:
                        eq = ccond.getRight();
                        break;
                    case NE:
                        ne = ccond.getRight();
                        break;
                    case LT:
                        hi = ccond.getRight();
                        hiInc = false;
                        break;
                    case LE:
                        hi = ccond.getRight();
                        hiInc = true;
                        break;
                    case GT:
                        lo = ccond.getRight();
                        loInc = false;
                        break;
                    case GE:
                        lo = ccond.getRight();
                        loInc = true;
                        break;
                    }
                }
                else if (cond instanceof InListCondition) {
                    in = ((InListCondition)cond).getExpressions();
                }
            }
            Histogram histogram = indexStatistics.getHistogram(0, 1);
            if (eq != null) {
                selectivity *= fractionEqual(column, index, histogram, eq);
            }
            else if (ne != null) 
                selectivity *= (1.0 - fractionEqual(column, index, histogram, eq));
            else if ((lo != null) || (hi != null))
                selectivity *= fractionBetween(column, index, histogram, lo, loInc, hi, hiInc);
            else if (in != null) {
                double fraction = 0.0;
                for (ExpressionNode expr : in) {
                    fraction += fractionEqual(column, index, histogram, expr);
                }
                if (fraction > 1.0) fraction = 1.0;
                selectivity *= fraction;
            }
        }
        return selectivity;
    }

    /** Estimate the cost of a sort of the given size. */
    public CostEstimate costSort(long size) {
        return new CostEstimate(size, model.sort((int)size, false));
    }

    /** Estimate the cost of a sort of the given size and limit. */
    public CostEstimate costSortWithLimit(long size, long limit, int nfields) {
        return new CostEstimate(Math.min(size, limit),
                                model.sortWithLimit((int)size, nfields));
    }

    /** Estimate cost of scanning the whole group. */
    // TODO: Need to account for tables actually wanted?
    public CostEstimate costGroupScan(Group group) {
        long nrows = 0;
        Table root = null;
        for (Table table : group.getRoot().getAIS().getTables().values()) {
            if (table.getGroup() == group) {
                if (table.getParentJoin() == null)
                    root = table;
                nrows += getTableRowCount(table);
            }
        }
        return new CostEstimate(nrows, model.fullGroupScan(schema.tableRowType(root)));
    }

    public CostEstimate costHKeyRow(List<ExpressionNode> keys) {
        double cost = model.project(keys.size(), 1);
        return new CostEstimate(1, cost);
    }

    public interface IndexIntersectionCoster {
        public CostEstimate singleIndexScanCost(SingleIndexScan scan, CostEstimator costEstimator);

    }

    private class IntersectionCostRunner {
        private IndexIntersectionCoster coster;
        private double cost = 0;
        private long rowCount = 0;

        private IntersectionCostRunner(IndexIntersectionCoster coster) {
            this.coster = coster;
        }

        public void buildIndexScanCost(IndexScan scan) {
            if (scan instanceof SingleIndexScan) {
                SingleIndexScan singleScan = (SingleIndexScan) scan;
                CostEstimate estimate = coster.singleIndexScanCost(singleScan, CostEstimator.this);
                long singleCount = estimate.getRowCount();
                double singleCost = estimate.getCost();
                if (rowCount == 0) {
                    // First index: start with its cost. This should
                    // be the output side, since nested intersections
                    // are left-deep. Its selectivity does not matter;
                    // subsequent ones filter it.
                    rowCount = singleCount;
                    cost = singleCost;
                }
                else {
                    // Add cost of this index and of intersecting its rows with rows so far.
                    cost += singleCost + model.intersect((int)rowCount, (int)singleCount);
                    long totalRowCount = getTableRowCount(singleScan.getIndex().leafMostTable());
                    if (totalRowCount > singleCount)
                        // Apply this index's selectivity to cumulative row count.
                        rowCount = simpleRound(rowCount * singleCount, totalRowCount);
                }
                rowCount = Math.max(rowCount, 1);
            }
            else if (scan instanceof MultiIndexIntersectScan) {
                MultiIndexIntersectScan multiScan = (MultiIndexIntersectScan) scan;
                buildIndexScanCost(multiScan.getOutputIndexScan());
                buildIndexScanCost(multiScan.getSelectorIndexScan());
            }
            else {
                throw new AssertionError("can't build scan of: " + scan);
            }
        }
    }

    public CostEstimate costValues(ExpressionsSource values, boolean selectToo) {
        int nfields = values.nFields();
        int nrows = values.getExpressions().size();
        double cost = model.project(nfields, nrows);
        if (selectToo)
            cost += model.select(nrows);
        CostEstimate estimate = new CostEstimate(nrows, cost);
        return adjustCostEstimate(estimate);
    }

    public CostEstimate costBoundRow(){
        return new CostEstimate(1,0);
    }

    public CostEstimate costBloomFilter(CostEstimate loaderCost,
                                        CostEstimate inputCost,
                                        CostEstimate checkCost,
                                        double checkSelectivity) {
        long checkCount = Math.max(Math.round(inputCost.getRowCount() * checkSelectivity),1);
        // Scan to load plus scan input plus check matching fraction
        // plus filter setup and use.
        CostEstimate estimate = 
               new CostEstimate(checkCount,
                                loaderCost.getCost() +
                                inputCost.getCost() +
                                // Model includes cost of one random access for check.
                             /* checkCost.getCost() * checkCount + */
                                model.selectWithFilter((int)inputCost.getRowCount(),
                                                       (int)loaderCost.getRowCount(),
                                                       checkSelectivity));
        return adjustCostEstimate(estimate);
    }

    public CostEstimate costHashLookup(CostEstimate equivalentCost,
                                       int joinColumns,
                                       int columnCount) {
        long nrows = equivalentCost.getRowCount();
        CostEstimate estimate = new CostEstimate(nrows,
                                                 model.unloadHashTable((int)nrows,
                                                                       joinColumns,
                                                                       columnCount));
        return adjustCostEstimate(estimate);
    }

    public CostEstimate costHashJoin(CostEstimate loaderCost,
                                     CostEstimate outerCost,
                                     CostEstimate lookupCost,
                                     int joinColumns,
                                     int outerColumnCount,
                                     int innerColumnCount) {
        CostEstimate estimate = outerCost.nest(lookupCost);
        estimate = new CostEstimate(estimate.getRowCount(),
                                    loaderCost.getCost() +
                                    model.loadHashTable((int)loaderCost.getRowCount(),
                                                        joinColumns,
                                                        outerColumnCount) +
                                    estimate.getCost());
        return adjustCostEstimate(estimate);
    }

    protected void missingStats(Index index, Column column) {
    }

    protected void checkRowCountChanged(Table table, IndexStatistics stats, 
                                        long rowCount) {
    }

}
