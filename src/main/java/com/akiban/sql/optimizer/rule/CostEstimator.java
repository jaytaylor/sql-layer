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

package com.akiban.sql.optimizer.rule;

import com.akiban.ais.model.*;
import com.akiban.sql.optimizer.rule.costmodel.CostModel;
import com.akiban.sql.optimizer.rule.costmodel.TableRowCounts;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;

import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
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
import static java.lang.Math.round;

public abstract class CostEstimator implements TableRowCounts
{
    private final Schema schema;
    private final Properties properties;
    private final CostModel model;
    private final Key key;
    private final PersistitKeyValueTarget keyTarget;
    private final Comparator<byte[]> bytesComparator;

    protected CostEstimator(Schema schema, Properties properties) {
        this.schema = schema;
        this.properties = properties;
        model = CostModel.newCostModel(schema, this);
        key = new Key((Persistit)null);
        keyTarget = new PersistitKeyValueTarget();
        bytesComparator = UnsignedBytes.lexicographicalComparator();
    }

    protected CostEstimator(SchemaRulesContext rulesContext) {
        this(rulesContext.getSchema(), rulesContext.getProperties());
    }

    @Override
    public long getTableRowCount(Table table) {
        // This implementation is only for testing; normally overridden by real server.
        // Return row count (not sample count) from analysis time.
        for (Index index : table.getIndexes()) {
            IndexStatistics istats = getIndexStatistics(index);
            if (istats != null)
                return istats.getRowCount();
        }
        return 1;
    }

    public abstract IndexStatistics getIndexStatistics(Index index);

    public void getIndexColumnStatistics(Index index, Index[] indexColumnsIndexes, IndexStatistics[] indexColumnsStats) {
        List<IndexColumn> allIndexColumns = index.getAllColumns();
        int i = 0;
        // For the first column, the index supplied by the optimizer is likely to be a better choice than an arbitrary
        // index with the right leading column.
        IndexStatistics statsForRequestedIndex = getIndexStatistics(index);
        if (statsForRequestedIndex != null) {
            indexColumnsIndexes[i] = index;
            indexColumnsStats[i++] = statsForRequestedIndex;
        }
        while (i < allIndexColumns.size()) {
            Index indexColumnsIndex = null;
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
                                      ExpressionNode highComparand, boolean highInclusive)
    {
        if (index.isUnique()) {
            if ((equalityComparands != null) &&
                (equalityComparands.size() >= index.getKeyColumns().size())) {
                // Exact match from unique index; probably one row.
                return indexAccessCost(1, index);
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
            return indexAccessCost(rowCount, index);
        }
        boolean scaleCount = true;
        double selectivity = 1.0;
        if (equalityComparands != null && !equalityComparands.isEmpty()) {
            selectivity = fractionEqual(equalityComparands, index,
                                        indexColumnsIndexes, indexColumnsStats);
        }
        if (lowComparand != null || highComparand != null) {
            selectivity *= fractionBetween(indexColumnsIndexes[columnCount - 1],
                                           indexColumnsStats[columnCount - 1],
                                           lowComparand, lowInclusive,
                                           highComparand, highInclusive);
        }
        if (mostlyDistinct(indexColumnsIndexes, indexColumnsStats)) scaleCount = false;
        // statsCount: Number of rows in the table based on an index of the table, according to index
        //    statistics, which may be stale.
        // rowCount: Approximate number of rows in the table, reasonably up to date.
        long statsCount = rowsInTableAccordingToIndex(indexedTable, indexColumnsIndexes, indexColumnsStats);
        if (statsCount <= 0) {
            statsCount = rowCount;
            scaleCount = false;
        }
        long nrows = Math.max(1, round(selectivity * statsCount));
        if (scaleCount)
            nrows = simpleRound((nrows * rowCount), statsCount);
        return indexAccessCost(nrows, index);
    }

    private long rowsInTableAccordingToIndex(UserTable indexedTable, Index[] indexColumnsIndexes, IndexStatistics[] indexColumnsStats)
    {
        // At least one of the index columns must be from the indexed table
        for (int i = 0; i < indexColumnsIndexes.length; i++) {
            Index index = indexColumnsIndexes[i];
            if (index != null) {
                Column leadingColumn = index.getKeyColumns().get(0).getColumn();
                if (leadingColumn.getTable() == indexedTable) {
                    return indexColumnsStats[i].getSampledCount();
                }
            }
        }
        // No index stats available.
        return -1;
    }
    
    // Estimate cost of fetching nrows from index.
    // One random access to get there, then nrows-1 sequential accesses following,
    // Plus a surcharge for copying something as wide as the index.
    private CostEstimate indexAccessCost(long nrows, Index index) {
        return new CostEstimate(nrows, 
                                model.indexScan(schema.indexRowType(index), (int)nrows));
    }

    protected double fractionEqual(List<ExpressionNode> eqExpressions, 
                                   Index index, 
                                   Index[] indexColumnsIndexes, IndexStatistics[] indexColumnsStats) {
        double selectivity = 1.0;
        keyTarget.attach(key);
        for (int column = 0; column < eqExpressions.size(); column++) {
            ExpressionNode node = eqExpressions.get(column);
            key.clear();
            // encodeKeyValue evaluates to true iff node is a constant expression. key is initialized as a side-effect.
            byte[] columnValue = encodeKeyValue(node, index, column) ? keyCopy() : null;
            selectivity *= fractionEqual(indexColumnsIndexes, indexColumnsStats, column, columnValue);
        }
        return selectivity;
    }
    
    protected double fractionEqual(Index[] indexColumnsIndexes, IndexStatistics[] indexColumnsStats, int column, byte[] columnValue) {
        Index index = indexColumnsIndexes[column];
        IndexStatistics indexStats = indexColumnsStats[column];
        if (indexStats == null) {
            return missingStatsSelectivity();
        } else {
            Histogram histogram = indexStats.getHistogram(1);
            if ((histogram == null) || histogram.getEntries().isEmpty()) {
                return missingStatsSelectivity();
            }
            else if (columnValue == null) {
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

    protected double fractionBetween(Index index, IndexStatistics indexStats,
                                     ExpressionNode lo, boolean lowInclusive,
                                     ExpressionNode hi, boolean highInclusive)
    {
        if (indexStats == null) {
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
        keyTarget.expectingType(index.getAllColumns().get(column).getColumn().getType().akType());
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
                branchCount++;
                rowCount *= nrows;
                // Cost of flattening these children with their ancestor.
                cost += model.flatten((int)nrows);
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
        long childCount = getTableRowCount(childNode.getTable().getTable().getTable());
        long ancestorCount = getTableRowCount(ancestorNode.getTable().getTable().getTable());
        if (ancestorCount == 0) return 1;
        return Math.max(simpleRound(childCount, ancestorCount), 1);
    }

    /** Estimate the cost of testing some conditions. */
    // TODO: Assumes that each condition turns into a separate select.
    public CostEstimate costSelect(Collection<ConditionExpression> conditions,
                                   long size) {
        return new CostEstimate(size, model.select((int)size) * conditions.size());
    }

    /** Estimate the cost of a sort of the given size. */
    public CostEstimate costSort(long size) {
        return new CostEstimate(size, model.sort((int)size, false));
    }

    /** Estimate cost of scanning the whole group. */
    // TODO: Need to account for tables actually wanted?
    public CostEstimate costGroupScan(Group group) {
        long nrows = 0;
        double cost = 0.0;
        UserTable root = null;
        for (UserTable table : group.getGroupTable().getAIS().getUserTables().values()) {
            if (table.getGroup() == group) {
                if (table.getParentJoin() == null)
                    root = table;
                nrows += getTableRowCount(table);
            }
        }
        return new CostEstimate(nrows, model.fullGroupScan(schema.userTableRowType(root)));
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
}
