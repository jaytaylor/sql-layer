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

import com.akiban.ais.model.TableIndex;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.rowtype.*;
import com.akiban.sql.optimizer.SimplifiedQuery.*;

import com.akiban.sql.parser.*;
import com.akiban.sql.compiler.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.sql.StandardException;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;

import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.service.EventTypes;
import com.akiban.server.service.instrumentation.SessionTracer;

import com.akiban.qp.expression.Comparison;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;

import com.akiban.qp.physicaloperator.PhysicalOperator;
import static com.akiban.qp.physicaloperator.API.*;

import com.akiban.qp.row.Row;

import java.util.*;

/**
 * Compile SQL statements into operator trees.
 */ 
public class OperatorCompiler
{
    protected SQLParserContext parserContext;
    protected NodeFactory nodeFactory;
    protected AISBinder binder;
    protected AISTypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected Grouper grouper;
    protected SchemaAISBased schema;

    public OperatorCompiler(SQLParser parser, 
                            AkibanInformationSchema ais, String defaultSchemaName) {
        parserContext = parser;
        nodeFactory = parserContext.getNodeFactory();
        binder = new AISBinder(ais, defaultSchemaName);
        parser.setNodeFactory(new BindingNodeFactory(nodeFactory));
        typeComputer = new AISTypeComputer();
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        grouper = new Grouper(parser);
        schema = new SchemaAISBased(ais);
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

    public ResultColumnBase getResultColumn(SimpleSelectColumn selectColumn) 
            throws StandardException {
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
            for (PhysicalOperator inputOperator : operator.getInputOperators()) {
                explainPlan(inputOperator, into, depth+1);
            }
        }
    }

    public Result compile(SessionTracer tracer, DMLStatementNode stmt, List<ParameterNode> params) throws StandardException {
        switch (stmt.getNodeType()) {
        case NodeTypes.CURSOR_NODE:
            return compileSelect(tracer, (CursorNode)stmt, params);
        case NodeTypes.UPDATE_NODE:
            return compileUpdate((UpdateNode)stmt, params);
        case NodeTypes.INSERT_NODE:
            return compileInsert((InsertNode)stmt, params);
        case NodeTypes.DELETE_NODE:
            return compileDelete((DeleteNode)stmt, params);
        default:
            throw new UnsupportedSQLException("Unsupported statement type: " + 
                                              stmt.statementToString());
        }
    }

    protected DMLStatementNode bindAndGroup(DMLStatementNode stmt) 
            throws StandardException {
        binder.bind(stmt);
        stmt = (DMLStatementNode)booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten(stmt);
        grouper.group(stmt);
        return stmt;
    }

    enum ProductMethod { HKEY_ORDERED, BY_RUN };

    public Result compileSelect(SessionTracer tracer, CursorNode cursor, List<ParameterNode> params) 
            throws StandardException {
        try {
            // Get into standard form.
            tracer.beginEvent(EventTypes.BIND_AND_GROUP);
            cursor = (CursorNode)bindAndGroup(cursor);
        } finally {
            tracer.endEvent();
        }
        SimplifiedSelectQuery squery = 
            new SimplifiedSelectQuery(cursor, grouper.getJoinConditions());
        squery.reorderJoins();
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
        if ((squery.getSortColumns() != null) &&
            !((index != null) && index.isSorting()))
            throw new UnsupportedSQLException("Unsupported ORDER BY: no suitable index on " + squery.getSortColumns());
        
        PhysicalOperator resultOperator;
        boolean needExtract = false;
        ProductMethod productMethod;
        if (index != null) {
            squery.removeConditions(index.getIndexConditions());
            squery.recomputeUsed();
            Index iindex = index.getIndex();
            TableNode indexTable = index.getTable();
            squery.getTables().setLeftBranch(indexTable);
            UserTableRowType tableType = tableRowType(indexTable);
            IndexRowType indexType = schema.indexRowType(iindex);
            resultOperator = indexScan_Default(indexType, 
                                               index.isReverse(),
                                               index.getIndexKeyRange());
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
            RowType ancestorInputType = indexType;
            boolean ancestorInputKept = false;
            if (descendantUsed) {
                resultOperator = branchLookup_Default(resultOperator, groupTable,
                                                      indexType, tableType, false);
                ancestorInputType = tableType; // Index no longer in stream.
                ancestorInputKept = tableUsed;
                needExtract = true; // Might be other descendants, too.
            }
            // Tables above this that also need to be output.
            List<RowType> addAncestors = new ArrayList<RowType>();
            // Any other branches need to be added beside the main one.
            List<TableNode> addBranches = new ArrayList<TableNode>();
            // Can use index's table if gotten from branch lookup or
            // needed via ancestor lookup.
            RowType branchInputType = (tableUsed || descendantUsed) ? tableType : null;
            for (TableNode left = indexTable; 
                 left != null; 
                 left = left.getParent()) {
                if ((left == indexTable) ?
                    (!descendantUsed && tableUsed) :
                    left.isUsed()) {
                    RowType atype = tableRowType(left);
                    addAncestors.add(atype);
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
                                    // indexType), but that is not
                                    // currently supported by either
                                    // operator.
                                    addAncestors.add(0, tableType);
                                    branchInputType = tableType;
                                }
                            }
                        }
                    }
                }
            }
            if (!addAncestors.isEmpty()) {
                resultOperator = ancestorLookup_Default(resultOperator, groupTable,
                                                        ancestorInputType, addAncestors, 
                                                        ancestorInputKept);
            }
            for (TableNode branchTable : addBranches) {
                resultOperator = branchLookup_Default(resultOperator, groupTable,
                                                      branchInputType, tableRowType(branchTable), 
                                                      true);
                needExtract = true; // Might bring in things not joined.
            }
            productMethod = ProductMethod.BY_RUN;
        }
        else {
            resultOperator = groupScan_Default(groupTable);
            needExtract = true; // Brings in the whole tree.
            productMethod = ProductMethod.HKEY_ORDERED;
        }
        
        // TODO: Can apply most Select conditions before flattening.
        // In addition to conditions between fields of different
        // tables, a left join should not be satisfied if the right
        // table has a failing condition, since the WHERE is on the
        // whole (as opposed to the outer join with a subquery
        // containing the condition).

        int nbranches = squery.getTables().colorBranches();
        Flattener fl = new Flattener(resultOperator, nbranches);
        FlattenState[] fls = null;
        try {
            tracer.beginEvent(EventTypes.FLATTEN);
            fls = fl.flatten(squery.getJoins());
        } finally {
            tracer.endEvent();
        }
        resultOperator = fl.getResultOperator();

        FlattenState fll = fls[0];
        RowType resultRowType = fll.getResultRowType();
        if (nbranches > 1) {
            // Product does not work if there are stray rows. Extract
            // their inputs (the flattened types) before attempting.
            Collection<RowType> extractTypes = new ArrayList<RowType>(nbranches);
            for (int i = 0; i < nbranches; i++) {
                extractTypes.add(fls[i].getResultRowType());
            }
            resultOperator = extract_Default(resultOperator, extractTypes);
            needExtract = false;

            for (int i = 1; i < nbranches; i++) {
                FlattenState flr = fls[i];
                switch (productMethod) {
                case BY_RUN:
                    resultOperator = product_ByRun(resultOperator,
                                                   resultRowType,
                                                   flr.getResultRowType());
                    break;
                default:
                    throw new UnsupportedSQLException("Need " + productMethod + 
                                                      " product of " +
                                                      resultRowType + " and " +
                                                      flr.getResultRowType());
                }
                resultRowType = resultOperator.rowType();
                fll.mergeTables(flr);
            }
        }
        Map<TableNode,Integer> fieldOffsets = fll.getFieldOffsets();

        if (needExtract) {
            // Now that we are done flattening, there is only one row type
            // that we need.  Extract it.
            resultOperator = extract_Default(resultOperator,
                                             Collections.singleton(resultRowType));
            // When selecting from a single table, we'll have that user
            // table type and not a flattened type.  If doing a group
            // scan, there may be descendants that survived the
            // extract. Cut them.
            if (resultRowType instanceof UserTableRowType) {
                UserTable table = ((UserTableRowType)resultRowType).userTable();
                if (!table.getChildJoins().isEmpty()) {
                    resultOperator = cut_Default(resultOperator, resultRowType);
                }
            }
        }

        for (ColumnCondition condition : squery.getConditions()) {
            Expression predicate = condition.generateExpression(fieldOffsets);
            resultOperator = select_HKeyOrdered(resultOperator,
                                                resultRowType,
                                                predicate);
        }

        int ncols = squery.getSelectColumns().size();
        List<ResultColumnBase> resultColumns = new ArrayList<ResultColumnBase>(ncols);
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
        resultRowType = resultOperator.rowType();

        int offset = squery.getOffset();
        int limit = squery.getLimit();

        return new Result(resultOperator, resultColumns, 
                          getParameterTypes(params),
                          offset, limit);
    }

    public Result compileUpdate(UpdateNode update, List<ParameterNode> params) 
            throws StandardException {
        update = (UpdateNode)bindAndGroup(update);
        SimplifiedUpdateStatement supdate = 
            new SimplifiedUpdateStatement(update, grouper.getJoinConditions());
        supdate.reorderJoins();

        TableNode targetTable = supdate.getTargetTable();
        GroupTable groupTable = targetTable.getGroupTable();
        UserTableRowType targetRowType = tableRowType(targetTable);
        
        // TODO: If we flattened subqueries (e.g., IN or EXISTS) into
        // the main result set, then the index and conditions might be
        // on joined tables, which might need to be looked up and / or
        // flattened in before non-index conditions.

        IndexUsage index = pickBestIndex(supdate);
        PhysicalOperator resultOperator;
        if (index != null) {
            assert (targetTable == index.getTable());
            supdate.removeConditions(index.getIndexConditions());
            Index iindex = index.getIndex();
            IndexRowType indexType = targetRowType.indexRowType(iindex);
            resultOperator = indexScan_Default(indexType, 
                                               index.isReverse(),
                                               index.getIndexKeyRange());
            List<RowType> ancestors = Collections.<RowType>singletonList(targetRowType);
            resultOperator = ancestorLookup_Default(resultOperator, groupTable,
                                                    indexType, ancestors, false);
        }
        else {
            resultOperator = groupScan_Default(groupTable);
        }
        
        Map<TableNode,Integer> fieldOffsets = new HashMap<TableNode,Integer>(1);
        fieldOffsets.put(targetTable, 0);
        for (ColumnCondition condition : supdate.getConditions()) {
            Expression predicate = condition.generateExpression(fieldOffsets);
            resultOperator = select_HKeyOrdered(resultOperator,
                                                targetRowType,
                                                predicate);
        }
        
        Expression[] updates = new Expression[targetRowType.nFields()];
        for (SimplifiedUpdateStatement.UpdateColumn updateColumn : 
                 supdate.getUpdateColumns()) {
            updates[updateColumn.getColumn().getPosition()] =
                updateColumn.getValue().generateExpression(fieldOffsets);
        }
        ExpressionRow updateRow = new ExpressionRow(targetRowType, updates);

        Plannable updatePlan = new com.akiban.qp.physicaloperator.Update_Default(resultOperator,
                                            new ExpressionRowUpdateFunction(updateRow));
        return new Result(updatePlan, getParameterTypes(params));
    }

    public Result compileInsert(InsertNode insert, List<ParameterNode> params) 
            throws StandardException {
        insert = (InsertNode)bindAndGroup(insert);
        SimplifiedInsertStatement sstmt = 
            new SimplifiedInsertStatement(insert, grouper.getJoinConditions());

        throw new UnsupportedSQLException("No Insert operators yet");
    }

    public Result compileDelete(DeleteNode delete, List<ParameterNode> params) 
            throws StandardException {
        delete = (DeleteNode)bindAndGroup(delete);
        SimplifiedDeleteStatement sstmt = 
            new SimplifiedDeleteStatement(delete, grouper.getJoinConditions());

        throw new UnsupportedSQLException("No Delete operators yet");
    }

    // A possible index.
    class IndexUsage implements Comparable<IndexUsage> {
        private TableNode table;
        private Index index;
        private List<ColumnCondition> equalityConditions;
        private ColumnCondition lowCondition, highCondition;
        private boolean sorting, reverse;
        
        public IndexUsage(Index index, TableNode table) {
            this.index = index;
            this.table = table; // Can be null: will be computed from columns.
        }

        public TableNode getTable() {
            return table;
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
            return getTable().getTable().getTableId().compareTo(other.getTable().getTable().getTableId());
        }

        // Can this index be used for part of the given query?
        public boolean usable(SimplifiedQuery squery) {
            List<IndexColumn> indexColumns = index.getColumns();
            if (index.isGroupIndex()) {
                // A group index is for the inner join, so all the
                // tables it indexes must appear in the query as well.
                // Its hkey is for the deepest table, figure out which
                // that is, too.
                TableNode deepest = null;
                for (IndexColumn indexColumn : indexColumns) {
                    TableNode table = squery.getColumnTable(indexColumn.getColumn());
                    if ((table == null) || !table.isUsed() || table.isOuter())
                        return false;
                    if ((deepest == null) || (deepest.getDepth() < table.getDepth()))
                        deepest = table;
                }
                if (table == null)
                    table = deepest;
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
            if (table.isUsed() && !table.isOuter()) {
                for (TableIndex index : table.getTable().getIndexes()) {
                    IndexUsage candidate = new IndexUsage(index, table);
                    bestIndex = betterIndex(squery, bestIndex, candidate);
                }
            }
        }
        for (GroupIndex index : squery.getGroup().getGroup().getIndexes()) {
            IndexUsage candidate = new IndexUsage(index, null);
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
        private RowType resultRowType;
        private List<TableNode> tables;
        private Map<TableNode,Integer> fieldOffsets;
        int nfields;

        public FlattenState(RowType resultRowType,
                            List<TableNode> tables,
                            Map<TableNode,Integer> fieldOffsets,
                            int nfields) {
            this.resultRowType = resultRowType;
            this.tables = tables;
            this.fieldOffsets = fieldOffsets;
            this.nfields = nfields;
        }
        
        public RowType getResultRowType() {
            return resultRowType;
        }
        public void setResultRowType(RowType resultRowType) {
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

        
        public void mergeTables(FlattenState other) {
            if (tables != null)
                tables.addAll(other.tables);
            for (TableNode table : other.fieldOffsets.keySet()) {
                fieldOffsets.put(table, other.fieldOffsets.get(table) + nfields);
            }
            nfields += other.nfields;
        }

        @Override
        public String toString() {
            return resultRowType.toString();
        }
    }

    // Holds a partial operator tree while flattening, since need the
    // single return value for above per-branch result.
    class Flattener implements Comparator<FlattenState> {
        private PhysicalOperator resultOperator;
        private int nbranches;
        private Map<List<RowType>,RowType> flattensDone;

        public Flattener(PhysicalOperator resultOperator, int nbranches) {
            this.resultOperator = resultOperator;
            this.nbranches = nbranches;
            if (nbranches > 1)
                flattensDone = new HashMap<List<RowType>,RowType>();
        }
        
        public PhysicalOperator getResultOperator() {
            return resultOperator;
        }

        public FlattenState[] flatten(BaseJoinNode join) {
            FlattenState[] result = new FlattenState[nbranches];
            for (int i = 0; i < nbranches; i++)
                result[i] = flatten(join, i);
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
                fleft.setResultRowType(flattenedType);
                fleft.mergeTables(fright);
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
    
    protected UserTableRowType tableRowType(TableNode table) {
        return schema.userTableRowType(table.getTable());
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
    protected Row getIndexExpressionRow(Index index, Expression[] keys) {
        RowType rowType = schema.indexRowType(index);
        return new ExpressionRow(rowType, keys);
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

}
