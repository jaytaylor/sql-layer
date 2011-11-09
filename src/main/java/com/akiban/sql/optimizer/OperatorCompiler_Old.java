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

package com.akiban.sql.optimizer;

import com.akiban.qp.operator.Operator;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.simplified.*;
import com.akiban.sql.optimizer.simplified.SimplifiedQuery.*;

import com.akiban.ais.model.TableIndex;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.expression.UnboundExpressions;
import com.akiban.qp.rowtype.*;
import com.akiban.qp.util.SchemaCache;

import com.akiban.sql.parser.*;
import com.akiban.sql.compiler.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.sql.StandardException;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;

import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.EventTypes;
import com.akiban.server.service.instrumentation.SessionTracer;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;

import static com.akiban.qp.operator.API.*;

import com.akiban.qp.row.Row;

import java.util.*;

/**
 * Compile SQL statements into operator trees.
 */ 
public class OperatorCompiler_Old
{
    protected SQLParserContext parserContext;
    protected NodeFactory nodeFactory;
    protected AISBinder binder;
    protected AISTypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected Grouper grouper;
    protected Schema schema;

    public OperatorCompiler_Old(SQLParser parser, 
                            AkibanInformationSchema ais, String defaultSchemaName) {
        parserContext = parser;
        nodeFactory = parserContext.getNodeFactory();
        binder = new AISBinder(ais, defaultSchemaName);
        parser.setNodeFactory(new BindingNodeFactory(nodeFactory));
        typeComputer = new AISTypeComputer();
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        grouper = new Grouper(parser);
        schema = SchemaCache.globalSchema(ais);
    }

    public void addView(ViewDefinition view) throws StandardException {
        binder.addView(view);
    }

    // Probably subclassed by specific client to capture typing information in some way.
    public static class ResultColumnBase {
        private String name;
        
        public ResultColumnBase(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public ResultColumnBase getResultColumn(SimpleSelectColumn selectColumn) {
        String name = selectColumn.getName();
        if (selectColumn.isNameDefaulted() && selectColumn.getExpression().isColumn())
            // Prefer the case stored in AIS to parser's standardized form.
            name = ((ColumnExpression)
                    selectColumn.getExpression()).getColumn().getName();
        return new ResultColumnBase(name);
    }

    public static class Result {
        private Plannable resultOperator;
        private List<ResultColumnBase> resultColumns;
        private DataTypeDescriptor[] parameterTypes;
        private int offset = 0;
        private int limit = -1;

        public Result(Plannable resultOperator,
                      List<ResultColumnBase> resultColumns,
                      DataTypeDescriptor[] parameterTypes,
                      int offset,
                      int limit) {
            this.resultOperator = resultOperator;
            this.resultColumns = resultColumns;
            this.parameterTypes = parameterTypes;
            this.offset = offset;
            this.limit = limit;
        }
        public Result(Plannable resultOperator,
                      DataTypeDescriptor[] parameterTypes) {
            this.resultOperator = resultOperator;
            this.parameterTypes = parameterTypes;
        }

        public Plannable getResultOperator() {
            return resultOperator;
        }
        public List<ResultColumnBase> getResultColumns() {
            return resultColumns;
        }
        public DataTypeDescriptor[] getParameterTypes() {
            return parameterTypes;
        }
        public int getOffset() {
            return offset;
        }
        public int getLimit() {
            return limit;
        }

        public boolean isModify() {
            return (resultColumns == null);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (String operator : explainPlan()) {
                if (sb.length() > 0) sb.append("\n");
                sb.append(operator);
            }
            if (resultColumns != null) {
                sb.append("\n");
                sb.append(resultColumns);
            }
            return sb.toString();
        }

        public List<String> explainPlan() {
            List<String> result = new ArrayList<String>();
            explainPlan(resultOperator, result, 0);
            return result;
        }

        protected static void explainPlan(Plannable operator,
                                          List<String> into, int depth) {
            
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < depth; i++)
                sb.append("  ");
            sb.append(operator);
            into.add(sb.toString());
            for (Operator inputOperator : operator.getInputOperators()) {
                explainPlan(inputOperator, into, depth+1);
            }
        }
    }

    public Result compile(SessionTracer tracer, DMLStatementNode stmt, List<ParameterNode> params) {
        try {
            // Get into standard form.
            tracer.beginEvent(EventTypes.BIND_AND_GROUP);
            stmt = bindAndGroup(stmt);
        } finally {
            tracer.endEvent();
        }
        switch (stmt.getNodeType()) {
        case NodeTypes.CURSOR_NODE:
            SimplifiedSelectQuery squery = 
                new SimplifiedSelectQuery((CursorNode)stmt, grouper.getJoinConditions());
            return compileSelect(tracer, squery, stmt, params);
        case NodeTypes.UPDATE_NODE:
        case NodeTypes.INSERT_NODE:
        case NodeTypes.DELETE_NODE:
            return CUDCompiler_Old.compileStatement(tracer, this, stmt, params);
        default:
            throw new UnsupportedSQLException (stmt.statementToString(), stmt);
        }
    }

    protected DMLStatementNode bindAndGroup(DMLStatementNode stmt)  {
        try {
            binder.bind(stmt);
            stmt = (DMLStatementNode)booleanNormalizer.normalize(stmt);
            typeComputer.compute(stmt);
            stmt = subqueryFlattener.flatten(stmt);
            grouper.group(stmt);
            return stmt;
        } catch (StandardException ex) {
            throw new com.akiban.server.error.ParseException ("", ex.getMessage(), stmt.toString());
        }
    }

    enum ProductMethod { HKEY_ORDERED, BY_RUN };

    static final int INSERTION_SORT_MAX_LIMIT = 100;

    public Result compileSelect(SessionTracer tracer, SimplifiedQuery squery, DMLStatementNode cursor, List<ParameterNode> params)  {
        squery.promoteImpossibleOuterJoins();
        GroupBinding group = squery.getGroup();
        GroupTable groupTable = group.getGroup().getGroupTable();
        
        // Try to use an index.
        IndexUsage index = null;
        try {
            tracer.beginEvent(EventTypes.PICK_BEST_INDEX);
            index = pickBestIndex(squery);
        } finally {
            tracer.endEvent();
        }
        
        Operator resultOperator;
        RowType resultRowType;
        ColumnExpressionToIndex fieldOffsets;
        boolean needFilter = false;
        covering: {
            IndexRowType indexRowType = null;
            ProductMethod productMethod;
            if (index != null) {
                squery.removeConditions(index.getIndexConditions());
                index.recomputeUsed();
                Index iindex = index.getIndex();
                indexRowType = schema.indexRowType(iindex);
                TableNode indexTable = index.getLeafMostTable();
                squery.getTables().setLeftBranch(indexTable);
                UserTableRowType tableType = tableRowType(indexTable);
                resultOperator = indexScan_Default(indexRowType, 
                                                   index.isReverse(),
                                                   index.getIndexKeyRange(),
                                                   tableRowType(index.getLeafMostRequired()));
                if (index.isCovering(squery)) {
                    resultRowType = indexRowType;
                    fieldOffsets = new ColumnIndexMap(index.getCoveringMap());
                    break covering;
                }
                // Decide whether to use BranchLookup, which gets all
                // descendants, or AncestorLookup, which gets just the
                // given type with the same number of B-tree accesses and
                // so is more efficient, for the index's target table.
                boolean tableUsed = false, descendantUsed = false;
                for (TableNode table : indexTable.subtree()) {
                    if (table == indexTable) {
                        tableUsed = table.isUsed();
                    }
                    else if (table.isUsed()) {
                        descendantUsed = true;
                        break;
                    }
                }
                RowType ancestorInputType = indexRowType;
                boolean ancestorInputKept = false;
                if (descendantUsed) {
                    resultOperator = branchLookup_Default(resultOperator, groupTable,
                                                          indexRowType, tableType, 
                                                          LookupOption.DISCARD_INPUT);
                    ancestorInputType = tableType; // Index no longer in stream.
                    ancestorInputKept = tableUsed;
                    needFilter = true; // Might be other descendants, too.
                }
                // Tables above this that also need to be output.
                List<TableNode> addAncestors = new ArrayList<TableNode>();
                List<RowType> addAncestorTypes = new ArrayList<RowType>();
                // Any other branches need to be added beside the main one.
                List<TableNode> addBranches = new ArrayList<TableNode>();
                // Can use index's table if gotten from branch lookup or
                // needed via ancestor lookup.
                RowType branchInputType = (tableUsed || descendantUsed) ? tableType 
                                                                        : null;
                for (TableNode left = indexTable; 
                     left != null; 
                     left = left.getParent()) {
                    if ((left == indexTable) ?
                        (!descendantUsed && tableUsed) :
                        left.isUsed()) {
                        RowType atype = tableRowType(left);
                        addAncestors.add(left);
                        addAncestorTypes.add(atype);
                        if (branchInputType == null)
                            branchInputType = atype;
                    }
                    {
                        TableNode sibling = left;
                        while (true) {
                            sibling = sibling.getNextSibling();
                            if (sibling == null) break;
                            if (sibling.subtreeUsed()) {
                                addBranches.add(sibling);
                                if (branchInputType == null) {
                                    // Need an input type for branch lookups. 
                                    // Prefer to take one that we're already looking up,
                                    // but can't go above the branchpoint.
                                    if ((sibling.getParent() == null) ||
                                        !sibling.getParent().isUsed()) {
                                        // Include the index's table in
                                        // ancestor lookup anyway so it
                                        // can be used for branch lookup.
                                        // TODO: Better might be to set
                                        // ancestorInputKept and use
                                        // ancestorInputType (i.e.,
                                        // indexRowType), but that is not
                                        // currently supported by either
                                        // operator.
                                        addAncestors.add(0, indexTable);
                                        addAncestorTypes.add(0, tableType);
                                        branchInputType = tableType;
                                    }
                                }
                            }
                        }
                    }
                }
                if (!addAncestors.isEmpty()) {
                    resultOperator = ancestorLookup_Default(resultOperator, groupTable,
                                                            ancestorInputType, 
                                                            addAncestorTypes, 
                                                            ancestorInputKept ? LookupOption.KEEP_INPUT : LookupOption.DISCARD_INPUT);
                    resultOperator = maybeAddTableConditions(resultOperator,
                                                             squery, addAncestors);
                }
                for (TableNode branchTable : addBranches) {
                    resultOperator = branchLookup_Default(resultOperator, groupTable,
                                                          branchInputType, 
                                                          tableRowType(branchTable), 
                                                          LookupOption.KEEP_INPUT);
                    resultOperator = maybeAddTableConditions(resultOperator,
                                                             squery, 
                                                             branchTable.subtree());
                    needFilter = true; // Might bring in things not joined.
                }
                productMethod = ProductMethod.BY_RUN;
            }
            else {
                resultOperator = groupScan_Default(groupTable);
                resultOperator = maybeAddTableConditions(resultOperator,
                                                         squery, squery.getTables());
                needFilter = true; // Brings in the whole tree.
                productMethod = ProductMethod.HKEY_ORDERED;
            }

            int nbranches = squery.getTables().colorBranches();
            if (nbranches > 0) {
                Flattener fl = new Flattener(resultOperator, nbranches);
                FlattenState[] fls;
                try {
                    tracer.beginEvent(EventTypes.FLATTEN);
                    fls = fl.flatten(squery);
                } 
                finally {
                    tracer.endEvent();
                }
                resultOperator = fl.getResultOperator();

                FlattenState fll = fls[0];
                resultRowType = fll.getResultRowType();
                if (nbranches > 1) {
                    // Product does not work if there are stray rows. Extract
                    // their inputs (the flattened types) before attempting.
                    Collection<RowType> extractTypes = new ArrayList<RowType>(nbranches);
                    for (int i = 0; i < nbranches; i++) {
                        extractTypes.add(fls[i].getResultRowType());
                    }
                    resultOperator = filter_Default(resultOperator, extractTypes);
                    needFilter = false;

                    for (int i = 1; i < nbranches; i++) {
                        FlattenState flr = fls[i];
                        switch (productMethod) {
                        case BY_RUN:
                            resultOperator = product_ByRun(resultOperator,
                                                           (AisRowType)resultRowType,
                                                           flr.getResultRowType());
                            break;
                        default:
                            throw new UnsupportedSQLException("Need " + productMethod + 
                                                              " product of " +
                                                              resultRowType + " and " +
                                                              flr.getResultRowType(), 
                                                                cursor);
                        }
                        resultRowType = (AisRowType) resultOperator.rowType();
                        fll.mergeTablesForProduct(flr);
                    }
                }
                fieldOffsets = new TableNodeOffsets(fll.getFieldOffsets());
            }
            else {
                // No branches happens when only constants are
                // selected from a index scan.  We just output them as
                // many times are there are index rows.
                resultRowType = indexRowType;
                fieldOffsets = new ColumnIndexMap(Collections.<Column,Integer>emptyMap());
            }
        }

        if (needFilter) {
            // Now that we are done flattening, there is only one row type
            // that we need.  Extract it.
            resultOperator = filter_Default(resultOperator,
                                             Collections.singleton(resultRowType));
        }

        for (ColumnCondition condition : squery.getConditions()) {
            Expression predicate = condition.generateExpression(fieldOffsets);
            resultOperator = select_HKeyOrdered(resultOperator,
                                                resultRowType,
                                                predicate);
        }

        if ((squery.getSortColumns() != null) &&
            !((index != null) && index.isSorting())) {
            int limit = squery.getLimit();
            if ((limit < 0) || (limit > INSERTION_SORT_MAX_LIMIT))
                throw new UnsupportedSQLException ("ORDER BY without index for " + squery.getSortColumns(), cursor);
            Ordering ordering = ordering();
            for (SortColumn sortColumn : squery.getSortColumns()) {
                ColumnExpression columnExpression = 
                    squery.getColumnExpression(sortColumn.getColumn());
                Expression sortExpression = 
                    columnExpression.generateExpression(fieldOffsets);
                ordering.append(sortExpression, sortColumn.isAscending());
            }
            resultOperator = sort_InsertionLimited(resultOperator, resultRowType, ordering, limit);
        }

        int ncols = squery.getSelectColumns().size();
        List<ResultColumnBase> resultColumns = new ArrayList<ResultColumnBase>(ncols);
        if ((ncols == 1) &&
            (squery.getSelectColumns().get(0).getExpression() instanceof 
             SimplifiedQuery.CountStarExpression)) {
            resultColumns.add(getResultColumn(squery.getSelectColumns().get(0)));
            resultOperator = count_Default(resultOperator, resultRowType);
        }
        else {
            List<Expression> resultExpressions = new ArrayList<Expression>(ncols);
            for (SimpleSelectColumn selectColumn : squery.getSelectColumns()) {
                ResultColumnBase resultColumn = getResultColumn(selectColumn);
                resultColumns.add(resultColumn);
                Expression resultExpression = 
                    selectColumn.getExpression().generateExpression(fieldOffsets);
                resultExpressions.add(resultExpression);
            }
            resultOperator = project_Default(resultOperator, resultRowType, 
                                             resultExpressions);
        }
        resultRowType = resultOperator.rowType();

        int offset = squery.getOffset();
        int limit = squery.getLimit();

        return new Result(resultOperator, resultColumns, 
                          getParameterTypes(params),
                          offset, limit);
    }

    // A possible index.
    class IndexUsage implements Comparable<IndexUsage> {
        private Index index;
        private TableNode rootMostTable, leafMostTable, leafMostRequired;
        private List<ColumnCondition> equalityConditions;
        private ColumnCondition lowCondition, highCondition;
        private boolean sorting, reverse;
        private Map<Column,Integer> coveringMap;

        public IndexUsage(TableIndex index, TableNode table) {
            this.index = index;
            rootMostTable = leafMostTable = leafMostRequired = table;
        }

        public IndexUsage(GroupIndex index) {
            this.index = index;
        }

        public TableNode getRootMostTable() {
            return rootMostTable;
        }
        public TableNode getLeafMostTable() {
            return leafMostTable;
        }
        public TableNode getLeafMostRequired() {
            return leafMostRequired;
        }

        public Index getIndex() {
            return index;
        }

        // The conditions that this index usage subsumes.
        public Set<ColumnCondition> getIndexConditions() {
            Set<ColumnCondition> result = new HashSet<ColumnCondition>();
            if (equalityConditions != null)
                result.addAll(equalityConditions);
            if (lowCondition != null)
                result.add(lowCondition);
            if (highCondition != null)
                result.add(highCondition);
            return result;
        }

        // Does this index accomplish the query's sorting?
        public boolean isSorting() {
            return sorting;
        }

        // Should the index iteration be in reverse?
        public boolean isReverse() {
            return reverse;
        }

        public Map<Column,Integer> getCoveringMap() {
            return coveringMap;
        }

        // Is this a better index?
        // TODO: Best we can do without any idea of selectivity.
        public int compareTo(IndexUsage other) {
            if (sorting) {
                if (!other.sorting)
                    // Sorted better than unsorted.
                    return +1;
            }
            else if (other.sorting)
                return -1;
            if (equalityConditions != null) {
                if (other.equalityConditions == null)
                    return +1;
                else if (equalityConditions.size() != other.equalityConditions.size())
                    return (equalityConditions.size() > other.equalityConditions.size()) 
                        // More conditions tested better than fewer.
                        ? +1 : -1;
            }
            else if (other.equalityConditions != null)
                return -1;
            int n = 0, on = 0;
            if (lowCondition != null)
                n++;
            if (highCondition != null)
                n++;
            if (other.lowCondition != null)
                on++;
            if (other.highCondition != null)
                on++;
            if (n != on) 
                return (n > on) ? +1 : -1;
            if (index.getColumns().size() != other.index.getColumns().size())
                    return (index.getColumns().size() < other.index.getColumns().size()) 
                        // Fewer columns indexed better than more.
                        ? +1 : -1;
            // Deeper better than shallower.
            return getLeafMostTable().getTable().getTableId().compareTo(other.getLeafMostTable().getTable().getTableId());
        }

        // Can this index be used for part of the given query?
        public boolean usable(SimplifiedQuery squery) {
            List<IndexColumn> indexColumns = index.getColumns();
            if (index.isGroupIndex()) {
                // A group index is for a left join, so we can use it
                // for joins that are inner from the root for a while
                // and then left from there to the leaf.
                UserTable indexRootTable = (UserTable)index.rootMostTable();
                UserTable indexLeafTable = (UserTable)index.leafMostTable();
                UserTable userTable = indexLeafTable;
                while (true) {
                    TableNode table = squery.getTables().getNode(userTable);
                    if ((table != null) && table.isUsed()) {
                        rootMostTable = table;
                        if (leafMostTable == null)
                            leafMostTable = table;
                        if ((leafMostRequired == null) && table.isRequired())
                            leafMostRequired = table;
                    }
                    else if ((userTable == indexLeafTable) ||
                             (userTable == indexRootTable))
                        // The leaf must be used or else we'll get
                        // duplicates from a scan (the indexed columns
                        // need not be root to leaf, making ancestors
                        // discontiguous and duplicates hard to
                        // eliminate). The root must be present, since
                        // the index does not contain orphans.
                        return false;
                    if (userTable == indexRootTable) {
                        if (leafMostRequired == null)
                            // Root-most is always in index.
                            return false;
                        break;
                    }
                    userTable = userTable.parentTable();
                }
            }
            int ncols = indexColumns.size();
            int nequals = 0;
            while (nequals < ncols) {
                IndexColumn indexColumn = indexColumns.get(nequals);
                Column column = indexColumn.getColumn();
                ColumnCondition equalityCondition = 
                    squery.findColumnConstantCondition(column, Comparison.EQ);
                if (equalityCondition == null)
                    break;
                if (nequals == 0)
                    equalityConditions = new ArrayList<ColumnCondition>(1);
                equalityConditions.add(equalityCondition);
                nequals++;
            }
            if (nequals < ncols) {
                IndexColumn indexColumn = indexColumns.get(nequals);
                Column column = indexColumn.getColumn();
                lowCondition = squery.findColumnConstantCondition(column, Comparison.GT);
                highCondition = squery.findColumnConstantCondition(column, Comparison.LT);
            }
            if (squery.getSortColumns() != null) {
                // Sort columns corresponding to equality constraints
                // were removed already as pointless. So the remaining
                // ones just need to be a subset of the remaining
                // index columns.
                int nsort = squery.getSortColumns().size();
                if (nsort <= (ncols - nequals)) {
                    found: {
                        for (int i = 0; i < nsort; i++) {
                            SortColumn sort = squery.getSortColumns().get(i);
                            IndexColumn index = indexColumns.get(nequals + i);
                            if (sort.getColumn() != index.getColumn())
                                break found;
                            // Iterate in reverse if index goes wrong way.
                            boolean dirMismatch = (sort.isAscending() != 
                                                   index.isAscending().booleanValue());
                            if (i == 0)
                                reverse = dirMismatch;
                            else if (reverse != dirMismatch)
                                break found;
                        }
                        // This index will accomplish required sorting.
                        sorting = true;
                    }
                }
            }
            return ((equalityConditions != null) ||
                    (lowCondition != null) ||
                    (highCondition != null) ||
                    sorting);
        }

        /** Clear used flags for tables all of whose conditions are
         * now taken care of by the index.
         * @see SimplifiedQuery#recomputeUsed */
        public void recomputeUsed() {
            TableNode table = leafMostTable;
            while (true) {
                boolean used = table.hasSelectColumns() || table.hasConditions();
                table.setUsed(used);
                if (table == rootMostTable)
                    break;
                if (used && table.isOptional())
                    // If there are select columns on an optional
                    // table (it can't be used due to conditions and
                    // optional), they need to come from an outer join
                    // row. So we still need an ancestor to join with.
                    // For now, just stop clearing entirely.
                    break;
                table = table.getParent();
            }
        }

        public boolean isCovering(SimplifiedQuery squery) {
            // For now, don't allow any more conditions.
            if (!squery.getConditions().isEmpty())
                return false;

            // No other tables can be joined in (they might be joined
            // to check against orphans, etc. without having select
            // columns).
            Set<TableNode> tables = new HashSet<TableNode>();
            {
                TableNode table = leafMostTable;
                while (true) {
                    tables.add(table);
                    if (table == rootMostTable)
                        break;
                    table = table.getParent();
                }
            }
            for (TableNode table : squery.getTables()) {
                if (table.isUsed() && !tables.contains(table))
                    return false;
            }
            
            Map<Column,Integer> columnOffsets = new HashMap<Column,Integer>();
            int nindexCols = index.getColumns().size();
            for (SimpleSelectColumn selectColumn : squery.getSelectColumns()) {
                SimpleExpression selectExpression = selectColumn.getExpression();
                if (selectExpression.isColumn()) {
                    Column column = ((ColumnExpression)selectExpression).getColumn();
                    found: {
                        for (int i = 0; i < nindexCols; i++) {
                            if (column == index.getColumns().get(i).getColumn()) {
                                columnOffsets.put(column, i);
                                break found;
                            }
                        }
                        return false;
                    }
                }
            }
            coveringMap = columnOffsets;
            return true;
        }

        // Generate key range bounds.
        public IndexKeyRange getIndexKeyRange() {
            if ((equalityConditions == null) &&
                (lowCondition == null) && (highCondition == null))
                return new IndexKeyRange(null, false, null, false);

            List<IndexColumn> indexColumns = index.getColumns();
            int nkeys = indexColumns.size();
            Expression[] keys = new Expression[nkeys];

            int kidx = 0;
            if (equalityConditions != null) {
                for (ColumnCondition cond : equalityConditions) {
                    keys[kidx++] = cond.getRight().generateExpression(null);
                }
            }
            for (int i = kidx; i < keys.length; ++i) {
                assert keys[i] == null : keys[i];
                keys[i] = Expressions.literal(null);
            }

            if ((lowCondition == null) && (highCondition == null)) {
                IndexBound eq = getIndexBound(index, keys, kidx);
                return new IndexKeyRange(eq, true, eq, true);
            }
            else {
                Expression[] lowKeys = null, highKeys = null;
                boolean lowInc = false, highInc = false;
                int lidx = kidx, hidx = kidx;
                if (lowCondition != null) {
                    lowKeys = keys;
                    if (highCondition != null) {
                        highKeys = new Expression[nkeys];
                        System.arraycopy(keys, 0, highKeys, 0, kidx);
                    }
                }
                else if (highCondition != null) {
                    highKeys = keys;
                }
                if (lowCondition != null) {
                    lowKeys[lidx++] = lowCondition.getRight().generateExpression(null);
                    lowInc = (lowCondition.getOperation() == Comparison.GE);
                }
                if (highCondition != null) {
                    highKeys[hidx++] = highCondition.getRight().generateExpression(null);
                    highInc = (highCondition.getOperation() == Comparison.LE);
                }
                IndexBound lo = getIndexBound(index, lowKeys, lidx);
                IndexBound hi = getIndexBound(index, highKeys, hidx);
                return new IndexKeyRange(lo, lowInc, hi, highInc);
            }
        }
    }

    // Pick an index to use.
    protected IndexUsage pickBestIndex(SimplifiedQuery squery) {
        if (squery.getConditions().isEmpty() && 
            (squery.getSortColumns() == null))
            return null;

        IndexUsage bestIndex = null;
        for (TableNode table : squery.getTables()) {
            if (table.isUsed() && !table.isOptional()) {
                for (TableIndex index : table.getTable().getIndexes()) {
                    IndexUsage candidate = new IndexUsage(index, table);
                    bestIndex = betterIndex(squery, bestIndex, candidate);
                }
            }
        }
        for (GroupIndex index : squery.getGroup().getGroup().getIndexes()) {
            IndexUsage candidate = new IndexUsage(index);
            bestIndex = betterIndex(squery, bestIndex, candidate);
            
        }
        return bestIndex;
    }

    protected IndexUsage betterIndex(SimplifiedQuery squery,
                                     IndexUsage bestIndex, IndexUsage candidate) {
        if (candidate.usable(squery)) {
            if ((bestIndex == null) ||
                (candidate.compareTo(bestIndex) > 0))
                return candidate;
        }
        return bestIndex;
    }

    static class FlattenState {
        private AisRowType resultRowType;
        private List<TableNode> tables;
        private Map<TableNode,Integer> fieldOffsets;
        int nfields;

        public FlattenState(AisRowType resultRowType,
                            List<TableNode> tables,
                            Map<TableNode,Integer> fieldOffsets,
                            int nfields) {
            this.resultRowType = resultRowType;
            this.tables = tables;
            this.fieldOffsets = fieldOffsets;
            this.nfields = nfields;
        }
        
        public AisRowType getResultRowType() {
            return resultRowType;
        }
        public void setResultRowType(AisRowType resultRowType) {
            this.resultRowType = resultRowType;
        }

        public List<TableNode> getTables() {
            return tables;
        }

        public Map<TableNode,Integer> getFieldOffsets() {
            return fieldOffsets;
        }
        public int getNfields() {
            return nfields;
        }

        
        public void mergeTablesForFlatten(FlattenState other) {
            if (tables != null)
                tables.addAll(other.tables);
            for (TableNode table : other.fieldOffsets.keySet()) {
                fieldOffsets.put(table, other.fieldOffsets.get(table) + nfields);
            }
            nfields += other.nfields;
        }

        public void mergeTablesForProduct(final FlattenState other) {
            // this and other have some tables in common. The result of the merge keeps the tables from this
            // and just the unique tables in other. The field offsets from other need to be "shifted down".
            // There could conceivably be multiple tables in common, all of which need to be removed, resulting in
            // different shift amounts for different offsets in other. For example, other could have tables
            // {A(offset 0, nfields 3), B(offset 3, nfields 3), C(offset 6, nfields 2), D(offset 8, nfields 2),
            // E(offset 10, nfields 2)}, with tables B and D also occurring in this. In this
            // case, C shifts down by 3, and E by 5.
            Set<TableNode> retained = new HashSet<TableNode>(other.tables);
            if (tables != null) {
                retained.removeAll(tables);
                tables.addAll(retained);
            }
            // Arrange other's tables in order of offset. This will simplify shifting later.
            List<TableNode> otherTables = new ArrayList<TableNode>(other.tables);
            Collections.sort(otherTables,
                             new Comparator<TableNode>() {
                                 @Override
                                 public int compare(TableNode x, TableNode y) {
                                     return other.fieldOffsets.get(x) - other.fieldOffsets.get(y);
                                 }
                             });
            // Compute new offsets of retained tables
            int accumulatedShift = 0;
            for (TableNode otherTable : otherTables) {
                if (retained.contains(otherTable))
                    fieldOffsets.put(otherTable, other.fieldOffsets.get(otherTable) + nfields - accumulatedShift);
                else
                    accumulatedShift += otherTable.getNFields();
            }
            nfields += other.nfields - accumulatedShift;
        }

        @Override
        public String toString() {
            return resultRowType.toString();
        }
    }

    // Holds a partial operator tree while flattening, since need the
    // single return value for above per-branch result.
    class Flattener implements Comparator<FlattenState> {
        private Operator resultOperator;
        private int nbranches;
        private Map<List<RowType>,RowType> flattensDone;

        public Flattener(Operator resultOperator, int nbranches) {
            this.resultOperator = resultOperator;
            this.nbranches = nbranches;
            if (nbranches > 1)
                flattensDone = new HashMap<List<RowType>,RowType>();
        }
        
        public Operator getResultOperator() {
            return resultOperator;
        }

        public FlattenState[] flatten(SimplifiedQuery squery) {
            FlattenState[] result = new FlattenState[nbranches];
            for (int i = 0; i < nbranches; i++) {
                BaseJoinNode joins = squery.getJoins();
                if (nbranches > 1)
                    // Get just one branch so that reordering is only
                    // within that and not confused by joins to
                    // another branch, which aren't flattened here.
                    joins = squery.isolateJoinNodeBranch(joins, (1 << i));
                joins = squery.reorderJoinNode(joins);
                // TODO: branch argument is now redundant, but kept for now.
                result[i] = flatten(joins, i);
            }
            if (nbranches > 1)
                Arrays.sort(result, this);
            return result;
        }

        public FlattenState flatten(BaseJoinNode join, int branch) {
            if (join.isTable()) {
                TableNode table = ((TableJoinNode)join).getTable();
                if (!table.isUsedOnBranch(branch))
                    return null;
                Map<TableNode,Integer> fieldOffsets = new HashMap<TableNode,Integer>();
                fieldOffsets.put(table, 0);
                List<TableNode> tables = null;
                if (nbranches > 1) {
                    tables = new ArrayList<TableNode>();
                    tables.add(table);
                }
                return new FlattenState(tableRowType(table),
                                        tables, fieldOffsets, table.getNFields());
            }
            else {
                JoinJoinNode jjoin = (JoinJoinNode)join;
                BaseJoinNode left = jjoin.getLeft();
                BaseJoinNode right = jjoin.getRight();
                FlattenState fleft = flatten(left, branch);
                FlattenState fright = flatten(right, branch);
                if (fleft == null) {
                    return fright;
                }
                else if (fright == null) {
                    return fleft;
                }
                RowType leftType = fleft.getResultRowType();
                RowType rightType = fright.getResultRowType();
                RowType flattenedType = null;
                List<RowType> flkey = null;
                if (flattensDone != null) {
                    // With overlapping branches processed from the
                    // root, it's possible that we've already done a
                    // common segment.
                    flkey = new ArrayList<RowType>(2);
                    flkey.add(leftType);
                    flkey.add(rightType);
                    flattenedType = flattensDone.get(flkey);
                }
                if (flattenedType == null) {
                    // Keep the parent side in multi-branch until the last one.
                    // TODO: May keep a few too many when the branch has
                    // multiple steps, (they do get extracted out).
                    EnumSet<FlattenOption> keep = 
                        ((nbranches > 1) && (branch < nbranches - 1)) ?
                        EnumSet.of(FlattenOption.KEEP_PARENT) :
                        EnumSet.noneOf(FlattenOption.class);
                    resultOperator = flatten_HKeyOrdered(resultOperator,
                                                         leftType, rightType,
                                                         jjoin.getJoinType(), keep);
                    flattenedType = resultOperator.rowType();
                    if (flattensDone != null) {
                        flattensDone.put(flkey, flattenedType);
                    }
                }
                fleft.setResultRowType((AisRowType)flattenedType);
                fleft.mergeTablesForFlatten(fright);
                return fleft;
            }
        }

        // Dictionary order on flattened table ordinals.
        public int compare(FlattenState fl1, FlattenState fl2) {
            List<TableNode> ts1 = fl1.getTables();
            List<TableNode> ts2 = fl2.getTables();
            int i = 0;
            assert ts1.get(i) == ts2.get(i) : "No common root";
            while (true) {
                i++;
                assert (i < ts1.size()) && (i < ts2.size()) : "Branch is subset";
                TableNode t1 = ts1.get(i);
                TableNode t2 = ts2.get(i);
                if (t1 != t2) {
                    return t1.getOrdinal() - t2.getOrdinal();
                }
            }
        }
    }
    
    protected Operator maybeAddTableConditions(Operator resultOperator,
                                                       SimplifiedQuery squery, 
                                                       Iterable<TableNode> tables) {
        for (TableNode table : tables) {
            // isRequired() because a WHERE condition (as opposed to
            // an JOIN ON condition) is for the whole flattened
            // row. Cutting half off before an OUTER join flatten
            // could output a row with nulls instead.
            // As it happens, conditions other than IS NULL imply required.
            if (table.isUsed() && table.hasConditions() && table.isRequired()) {
                RowType tableRowType = tableRowType(table);
                Map<TableNode,Integer> tableOffsets = new HashMap<TableNode,Integer>(1);
                tableOffsets.put(table, 0);
                ColumnExpressionToIndex fieldOffsets = new TableNodeOffsets(tableOffsets);
                for (ColumnCondition condition : table.getConditions()) {
                    // Condition must not require another table.
                    // (Don't bother yet trying to test those as soon
                    // as the flatten that has them both is done.)
                    if (condition.isSingleTable()) {
                        Expression predicate = condition.generateExpression(fieldOffsets);
                        resultOperator = select_HKeyOrdered(resultOperator,
                                                            tableRowType,
                                                            predicate);
                        squery.getConditions().remove(condition);
                    }
                }
            }
        }
        return resultOperator;
    }

    protected UserTableRowType tableRowType(TableNode table) {
        return schema.userTableRowType(table.getTable());
    }

    protected ValuesRowType valuesRowType (AkType[] fields) {
        return schema.newValuesType(fields);
    }
    
    /** Return an index bound for the given index and expressions.
     * @param index the index in use
     * @param keys {@link Expression}s for index lookup key
     * @param nkeys number of keys actually in use
     */
    protected IndexBound getIndexBound(Index index, Expression[] keys, int nkeys) {
        if (keys == null) 
            return null;
        return new IndexBound(getIndexExpressionRow(index, keys),
                              getIndexColumnSelector(index, nkeys));
    }

    /** Return a column selector that enables the first <code>nkeys</code> fields
     * of a row of the index's user table. */
    protected ColumnSelector getIndexColumnSelector(final Index index, final int nkeys) {
        assert nkeys <= index.getColumns().size() : index + " " + nkeys;
        return new ColumnSelector() {
                public boolean includesColumn(int columnPosition) {
                    return columnPosition < nkeys;
                }
            };
    }

    /** Return a {@link Row} for the given index containing the given
     * {@link Expression} values.  
     */
    protected UnboundExpressions getIndexExpressionRow(Index index, Expression[] keys) {
        RowType rowType = schema.indexRowType(index);
        return new RowBasedUnboundExpressions(rowType, Arrays.asList(keys));
    }

    protected DataTypeDescriptor[] getParameterTypes(List<ParameterNode> params) {
        if ((params == null) || params.isEmpty())
            return null;
        int nparams = 0;
        for (ParameterNode param : params) {
            if (nparams < param.getParameterNumber() + 1)
                nparams = param.getParameterNumber() + 1;
        }
        DataTypeDescriptor[] result = new DataTypeDescriptor[nparams];
        for (ParameterNode param : params) {
            result[param.getParameterNumber()] = param.getType();
        }        
        return result;
    }
    
    protected Set<ValueNode> getJoinConditions() { 
        return grouper.getJoinConditions(); 
    }
}
