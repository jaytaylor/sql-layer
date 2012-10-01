/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.rule.cost;

import com.akiban.sql.optimizer.rule.SchemaRulesContext;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;

import com.akiban.ais.model.*;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.store.statistics.IndexStatistics;
import static com.akiban.server.store.statistics.IndexStatistics.*;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.Persistit;

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
    private final PersistitKeyValueTarget keyTarget;
    private final Comparator<byte[]> bytesComparator;
    protected boolean warningsEnabled;

    protected CostEstimator(Schema schema, Properties properties, KeyCreator keyCreator) {
        this.schema = schema;
        this.properties = properties;
        model = CostModel.newCostModel(schema, this);
        key = keyCreator.createKey();
        keyTarget = new PersistitKeyValueTarget();
        bytesComparator = UnsignedBytes.lexicographicalComparator();
        warningsEnabled = logger.isWarnEnabled();
    }

    protected CostEstimator(SchemaRulesContext rulesContext, KeyCreator keyCreator) {
        this(rulesContext.getSchema(), rulesContext.getProperties(), keyCreator);
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

    public void getIndexColumnStatistics(Index index, Index[] indexColumnsIndexes, IndexStatistics[] indexColumnsStats) {
        List<IndexColumn> allIndexColumns = index.getAllColumns();
        int i = 0;
        // For the first column, the index supplied by the optimizer is likely to be a better choice than an arbitrary
        // index with the right leading column.
        indexColumnsIndexes[i] = index;
        IndexStatistics statsForRequestedIndex = getIndexStatistics(index);
        if (statsForRequestedIndex != null) {
            indexColumnsStats[i++] = statsForRequestedIndex;
        }
        while (i < allIndexColumns.size()) {
            Index indexColumnsIndex = (i == 0) ? index : null;
            IndexStatistics indexStatistics = null;
            Column leadingColumn = allIndexColumns.get(i).getColumn();
            // Find a TableIndex whose first column is leadingColumn
            for (TableIndex tableIndex : leadingColumn.getTable().getIndexes()) {
                if (tableIndex.getKeyColumns().get(0).getColumn() == leadingColumn) {
                    indexStatistics = getIndexStatistics(tableIndex);
                    if (indexStatistics != null) {
                        indexColumnsIndex = tableIndex;
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
                                break groupLoop;
                            }
                            else if (indexColumnsIndex == null) {
                                indexColumnsIndex = groupIndex;
                            }
                        }
                    }
                }
            }
            indexColumnsIndexes[i] = indexColumnsIndex;
            indexColumnsStats[i++] = indexStatistics;
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
        UserTable indexedTable = (UserTable) index.leafMostTable();
        long rowCount = getTableRowCount(indexedTable);
        // Get IndexStatistics for each column. If the ith element is non-null, then it definitely has
        // a leading-column histogram (obtained by IndexStats.getHistogram(1)).
        // else: There are no index stats for the first column of the index. Either there is no such index,
        // or there is, but it doesn't have stats.
        int nidxcols = index.getAllColumns().size();
        Index[] indexColumnsIndexes = new Index[nidxcols];
        IndexStatistics[] indexColumnsStats = new IndexStatistics[nidxcols];
        getIndexColumnStatistics(index, indexColumnsIndexes, indexColumnsStats);
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
                                        indexColumnsIndexes, indexColumnsStats,
                                        equalityComparands);
        }
        if (lowComparand != null || highComparand != null) {
            selectivity *= fractionBetween(index.getAllColumns().get(columnCount - 1).getColumn(),
                                           indexColumnsIndexes[columnCount - 1],
                                           indexColumnsStats[columnCount - 1],
                                           lowComparand, lowInclusive,
                                           highComparand, highInclusive);
        }
        if (mostlyDistinct(indexColumnsIndexes, indexColumnsStats)) scaleCount = false;
        // statsCount: Number of rows in the table based on an index of the table, according to index
        //    statistics, which may be stale.
        // rowCount: Approximate number of rows in the table, reasonably up to date.
        long statsCount;
        IndexStatistics stats = tableIndexStatistics(indexedTable, indexColumnsIndexes, indexColumnsStats);
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

    private IndexStatistics tableIndexStatistics(UserTable indexedTable, Index[] indexColumnsIndexes, IndexStatistics[] indexColumnsStats) {
        // At least one of the index columns must be from the indexed table
        for (int i = 0; i < indexColumnsIndexes.length; i++) {
            Index index = indexColumnsIndexes[i];
            if (index != null) {
                Column leadingColumn = index.getKeyColumns().get(0).getColumn();
                if (leadingColumn.getTable() == indexedTable) {
                    return indexColumnsStats[i];
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
                                   Index[] indexColumnsIndexes, IndexStatistics[] indexColumnsStats,
                                   List<ExpressionNode> eqExpressions) {
        double selectivity = 1.0;
        keyTarget.attach(key);
        for (int column = 0; column < eqExpressions.size(); column++) {
            ExpressionNode node = eqExpressions.get(column);
            selectivity *= fractionEqual(index.getAllColumns().get(column).getColumn(),
                                         indexColumnsIndexes[column], indexColumnsStats[column],
                                         node);
        }
        return selectivity;
    }

    protected double fractionEqual(Column column, 
                                   Index index, IndexStatistics indexStats,
                                   ExpressionNode expr) {
        if (indexStats == null) {
            missingStats(column, index);
            return missingStatsSelectivity();
        } else {
            Histogram histogram = indexStats.getHistogram(1);
            if ((histogram == null) || histogram.getEntries().isEmpty()) {
                missingStats(column, index);
                return missingStatsSelectivity();
            } else {
                key.clear();
                keyTarget.attach(key);
                // encodeKeyValue evaluates non-null iff node is a constant expression. key is initialized as a side-effect.
                byte[] columnValue = encodeKeyValue(expr, index, 0) ? keyCopy() : null;
                if (columnValue == null) {
                    // Variable expression. Use average selectivity for histogram.
                    return
                        mostlyDistinct(indexStats)
                        ? 1.0 / indexStats.getSampledCount()
                        : 1.0 / histogram.totalDistinctCount();
                } else {
                    // TODO: Could use Collections.binarySearch if we had something that looked like a HistogramEntry.
                    List<HistogramEntry> entries = histogram.getEntries();
                    for (HistogramEntry entry : entries) {
                        // Constant expression
                        int compare = bytesComparator.compare(columnValue, entry.getKeyBytes());
                        if (compare == 0) {
                            return ((double) entry.getEqualCount()) / indexStats.getSampledCount();
                        } else if (compare < 0) {
                            long d = entry.getDistinctCount();
                            return d == 0 ? 0.0 : ((double) entry.getLessCount()) / (d * indexStats.getSampledCount());
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
                                     Index index, IndexStatistics indexStats,
                                     ExpressionNode lo, boolean lowInclusive,
                                     ExpressionNode hi, boolean highInclusive)
    {
        if (indexStats == null) {
            missingStats(column, index);
            return missingStatsSelectivity();
        }
        keyTarget.attach(key);
        key.clear();
        byte[] loBytes = encodeKeyValue(lo, index, 0) ? keyCopy() : null;
        key.clear();
        byte[] hiBytes = encodeKeyValue(hi, index, 0) ? keyCopy() : null;
        if (loBytes == null && hiBytes == null) {
            return missingStatsSelectivity();
        }
        Histogram histogram = indexStats.getHistogram(1);
        if ((histogram == null) || histogram.getEntries().isEmpty()) {
            missingStats(column, index);
            return missingStatsSelectivity();
        }
        boolean before = (loBytes != null);
        long rowCount = 0;
        byte[] entryStartBytes, entryEndBytes = null;
        HistogramEntry firstEntry = histogram.getEntries().get(0);
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
        return ((double) Math.max(rowCount, 1)) / indexStats.getSampledCount();
    }


    // Must be provably mostly distinct: Every histogram is available and mostly distinct.
    private boolean mostlyDistinct(Index[] indexColumnsIndexes,
                                   IndexStatistics[] indexColumnsStats)
    {
        for (IndexStatistics indexStats : indexColumnsStats) {
            if (indexStats == null) {
                return false;
            } else {
                if (!mostlyDistinct(indexStats)) {
                    // < 90% distinct
                    return false;
                }
            }
        }
        return true;
    }

    private boolean mostlyDistinct(IndexStatistics indexStats)
    {
        Histogram histogram = indexStats.getHistogram(1);
        if (histogram == null) return false;
        return histogram.totalDistinctCount() * 10 > indexStats.getSampledCount() * 9;
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
        else if (upper) {
            key.append(Key.AFTER);
        }
        return keyCopy();
    }    
    
    protected boolean isConstant(ExpressionNode node)
    {
        return node instanceof ConstantExpression;
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
        if (index.isSpatial()) {
            keyTarget.expectingType(AkType.LONG, null);
        }
        else {
            keyTarget.expectingType(index.getAllColumns().get(column).getColumn());
        }
        Converters.convert(valueSource, keyTarget);
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
        List<UserTableRowType> ancestorTypes = new ArrayList<UserTableRowType>();
        for (TableGroupJoinNode ancestorNode = startNode;
             ancestorNode != null;
             ancestorNode = ancestorNode.getParent()) {
            if (isRequired(ancestorNode)) {
                if ((ancestorNode == startNode) &&
                    (getSideBranches(ancestorNode) != 0)) {
                    continue;   // Branch, not ancestor.
                }
                ancestorTypes.add(schema.userTableRowType(ancestorNode.getTable().getTable().getTable()));
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
                cost += model.branchLookup(schema.userTableRowType(nextToRoot.getTable().getTable().getTable()));
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
                                                       Map<UserTable,Long> tableCounts) {
        TableGroupJoinNode rootNode = tableGroup.getRoot();
        coverBranches(tableGroup, rootNode, requiredTables);
        int branchCount = 0;
        long rowCount = 1;
        double cost = 0.0;
        for (Map.Entry<UserTable,Long> entry : tableCounts.entrySet()) {
            cost += model.partialGroupScan(schema.userTableRowType(entry.getKey()), 
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
            cost += model.ancestorLookup(Collections.singletonList(schema.userTableRowType(insideTable.getTable().getTable())));
        }
        else {
            rowCount *= descendantCardinality(insideTable, outsideTable);
            cost += model.branchLookup(schema.userTableRowType(insideTable.getTable().getTable()));
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
        return new CostEstimate(Math.max(1, round(size * selectivity)),
                                model.select((int)size) * conditions.size());
    }

    public CostEstimate costSelect(Collection<ConditionExpression> conditions,
                                   Map<ColumnExpression,Collection<ComparisonCondition>> selectivityConditions,
                                   long size) {
        return costSelect(conditions, conditionsSelectivity(selectivityConditions), size);
    }

    public double conditionsSelectivity(Map<ColumnExpression,Collection<ComparisonCondition>> conditions) {
        double selectivity = 1.0;
        for (Map.Entry<ColumnExpression,Collection<ComparisonCondition>> entry : conditions.entrySet()) {
            Index index = null;
            IndexStatistics indexStatistics = null;
            Column column = entry.getKey().getColumn();
            // Find a TableIndex whose first column is leadingColumn
            for (TableIndex tableIndex : column.getTable().getIndexes()) {
                if (tableIndex.getKeyColumns().get(0).getColumn() == column) {
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
                        if (groupIndex.getKeyColumns().get(0).getColumn() == column) {
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
            for (ComparisonCondition cond : entry.getValue()) {
                switch (cond.getOperation()) {
                case EQ:
                    eq = cond.getRight();
                    break;
                case NE:
                    ne = cond.getRight();
                    break;
                case LT:
                    hi = cond.getRight();
                    hiInc = false;
                    break;
                case LE:
                    hi = cond.getRight();
                    hiInc = true;
                    break;
                case GT:
                    lo = cond.getRight();
                    loInc = false;
                    break;
                case GE:
                    lo = cond.getRight();
                    loInc = true;
                    break;
                }
            }
            if (eq != null)
                selectivity *= fractionEqual(column, index, indexStatistics, eq);
            else if (ne != null) 
                selectivity *= (1.0 - fractionEqual(column, index, indexStatistics, eq));
            else if ((lo != null) || (hi != null))
                selectivity *= fractionBetween(column, index, indexStatistics, lo, loInc, hi, hiInc);
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
        double cost = 0.0;
        UserTable root = null;
        for (UserTable table : group.getRoot().getAIS().getUserTables().values()) {
            if (table.getGroup() == group) {
                if (table.getParentJoin() == null)
                    root = table;
                nrows += getTableRowCount(table);
            }
        }
        return new CostEstimate(nrows, model.fullGroupScan(schema.userTableRowType(root)));
    }

    public CostEstimate costBloomFilter(CostEstimate loaderCost,
                                        CostEstimate inputCost,
                                        CostEstimate checkCost,
                                        double checkSelectivity) {
        long checkCount = Math.max(Math.round(inputCost.getRowCount() * checkSelectivity),1);
        // Scan to load plus scan input plus check matching fraction
        // plus filter setup and use.
        return new CostEstimate(checkCount,
                                loaderCost.getCost() +
                                inputCost.getCost() +
                                // Model includes cost of one random access for check.
                             /* checkCost.getCost() * checkCount + */
                                model.selectWithFilter((int)inputCost.getRowCount(),
                                                       (int)loaderCost.getRowCount(),
                                                       checkSelectivity));
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

    protected void missingStats(Column column, Index index) {
        if (warningsEnabled) {
            if (index == null) {
                logger.warn("No single column index for {}.{}; cost estimates will not be accurate", column.getTable().getName(), column.getName());
            }
            else if (index.isTableIndex()) {
                Table table = ((TableIndex)index).getTable();
                logger.warn("No statistics for table {}; cost estimates will not be accurate", table.getName());
            }
            else {
                logger.warn("No statistics for index {}; cost estimates will not be accurate", index.getIndexName());
            }
        }
    }

    public static final double MIN_ROW_COUNT_SMALLER = 0.2;
    public static final double MAX_ROW_COUNT_LARGER = 5.0;

    protected void checkRowCountChanged(UserTable table, IndexStatistics stats, 
                                        long rowCount) {
        if (warningsEnabled) {
            double ratio = (double)Math.max(rowCount, 1) / 
                           (double)Math.max(stats.getRowCount(), 1);
            String msg = null;
            long change = 1;
            if (ratio < MIN_ROW_COUNT_SMALLER) {
                msg = "smaller";
                change = Math.round(1.0 / ratio);
            }
            else if (ratio > MAX_ROW_COUNT_LARGER) {
                msg = "larger";
                change = Math.round(ratio);
            }
            if (msg != null) {
                logger.warn("Table {} is {} times {} than on {}; cost estimates will not be accurate until statistics are updated", new Object[] { table.getName(), change, msg, new Date(stats.getAnalysisTimestamp()) });
            }
        }
    }

}
