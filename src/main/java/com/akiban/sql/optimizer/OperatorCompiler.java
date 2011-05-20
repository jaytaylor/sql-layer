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

import com.akiban.sql.optimizer.SimplifiedSelectQuery.*;

import com.akiban.sql.parser.*;
import com.akiban.sql.compiler.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

import com.akiban.server.api.dml.ColumnSelector;

import com.akiban.qp.expression.Comparison;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import static com.akiban.qp.expression.API.*;

import com.akiban.qp.physicaloperator.PhysicalOperator;
import static com.akiban.qp.physicaloperator.API.*;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;

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
    protected Schema schema;

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
        schema = new Schema(ais);
    }

    public void addView(ViewDefinition view) throws StandardException {
        binder.addView(view);
    }

    public static class Result {
        private PhysicalOperator resultOperator;
        private RowType resultRowType;
        private List<Column> resultColumns;
        private int[] resultColumnOffsets;

        public Result(PhysicalOperator resultOperator,
                      RowType resultRowType,
                      List<Column> resultColumns,
                      int[] resultColumnOffsets) {
            this.resultOperator = resultOperator;
            this.resultRowType = resultRowType;
            this.resultColumns = resultColumns;
            this.resultColumnOffsets = resultColumnOffsets;
        }

        public PhysicalOperator getResultOperator() {
            return resultOperator;
        }
        public RowType getResultRowType() {
            return resultRowType;
        }
        public List<Column> getResultColumns() {
            return resultColumns;
        }
        public int[] getResultColumnOffsets() {
            return resultColumnOffsets;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (String operator : explainPlan()) {
                if (sb.length() > 0) sb.append("\n");
                sb.append(operator);
            }
            return sb.toString();
        }

        public List<String> explainPlan() {
            List<String> result = new ArrayList<String>();
            explainPlan(resultOperator, result, 0);
            return result;
        }

        protected static void explainPlan(PhysicalOperator operator, 
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

    public Result compile(CursorNode cursor) throws StandardException {
        // Get into standard form.
        binder.bind(cursor);
        cursor = (CursorNode)booleanNormalizer.normalize(cursor);
        typeComputer.compute(cursor);
        cursor = (CursorNode)subqueryFlattener.flatten(cursor);
        grouper.group(cursor);

        SimplifiedSelectQuery squery = 
            new SimplifiedSelectQuery(cursor, grouper.getJoinConditions());
        GroupBinding group = squery.getGroup();
        GroupTable groupTable = group.getGroup().getGroupTable();
        
        // Try to use an index.
        IndexUsage index = pickBestIndex(squery);
        if (squery.getSortColumns() != null)
            throw new UnsupportedSQLException("Unsupported ORDER BY");
        
        Set<ColumnCondition> indexConditions = null;
        PhysicalOperator resultOperator;
        if (index != null) {
            indexConditions = index.getIndexConditions();
            Index iindex = index.getIndex();
            PhysicalOperator indexOperator = indexScan_Default(iindex, 
                                                               index.isReverse(),
                                                               index.getIndexKeyRange());
            UserTable indexTable = (UserTable)iindex.getTable();
            UserTableRowType tableType = userTableRowType(indexTable);
            IndexRowType indexType = tableType.indexRowType(iindex);
            resultOperator = lookup_Default(indexOperator, groupTable,
                                            indexType, tableType);
            // All selected rows above this need to be output by hkey left
            // segment random access.
            List<RowType> addAncestors = new ArrayList<RowType>();
            for (UserTable table : squery.getTables()) {
                if ((table != indexTable) && isAncestorTable(table, indexTable))
                    addAncestors.add(userTableRowType(table));
            }
            if (!addAncestors.isEmpty())
                resultOperator = ancestorLookup_Default(resultOperator, groupTable,
                                                        tableType, addAncestors);
        }
        else {
            resultOperator = groupScan_Default(groupTable);
        }
        
        // TODO: Can apply most Select conditions before flattening.
        // In addition to conditions between fields of different
        // tables, a left join should not be satisfied if the right
        // table has a failing condition, since the WHERE is on the
        // whole (as opposed to the outer join with a subquery
        // containing the condition).

        Flattener fl = new Flattener(resultOperator);
        FlattenState fls = fl.flatten(squery.getJoins());
        resultOperator = fl.getResultOperator();
        RowType resultRowType = fls.getResultRowType();
        Map<UserTable,Integer> fieldOffsets = fls.getFieldOffsets();
        
        for (ColumnCondition condition : squery.getConditions()) {
            if ((indexConditions != null) && indexConditions.contains(condition))
                continue;
            Expression predicate = condition.generateExpression(fieldOffsets);
            resultOperator = select_HKeyOrdered(resultOperator,
                                                resultRowType,
                                                predicate);
        }

        int ncols = squery.getSelectColumns().size();
        List<Column> resultColumns = new ArrayList<Column>(ncols);
        for (SelectColumn selectColumn : squery.getSelectColumns()) {
            resultColumns.add(selectColumn.getColumn());
        }
        int[] resultColumnOffsets = new int[ncols];
        for (int i = 0; i < ncols; i++) {
            Column column = resultColumns.get(i);
            UserTable table = column.getUserTable();
            resultColumnOffsets[i] = fieldOffsets.get(table) + column.getPosition();
        }

        return new Result(resultOperator, resultRowType, 
                          resultColumns, resultColumnOffsets);
    }

    // A possible index.
    class IndexUsage implements Comparable<IndexUsage> {
        private Index index;
        private List<ColumnCondition> equalityConditions;
        ColumnCondition lowCondition, highCondition;
        
        public IndexUsage(Index index) {
            this.index = index;
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

        public boolean isReverse() {
            return false;
        }

        // Is this a better index?
        // TODO: Best we can do without any idea of selectivity.
        public int compareTo(IndexUsage other) {
            if (equalityConditions != null) {
                if (other.equalityConditions == null)
                    return +1;
                else if (equalityConditions.size() != other.equalityConditions.size())
                    return (equalityConditions.size() > other.equalityConditions.size()) 
                        ? +1 : -1;
            }
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
            return index.getTable().getTableId().compareTo(other.index.getTable().getTableId());
        }

        // Can this index be used for part of the given query?
        public boolean usable(SimplifiedSelectQuery squery) {
            List<IndexColumn> indexColumns = index.getColumns();
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
            return ((equalityConditions != null) ||
                    (lowCondition != null) ||
                    (highCondition != null));
        }

        // Generate key range bounds.
        public IndexKeyRange getIndexKeyRange() {
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
                IndexBound eq = getIndexBound(index, keys);
                return new IndexKeyRange(eq, true, eq, true);
            }
            else {
                Expression[] lowKeys = null, highKeys = null;
                boolean lowInc = false, highInc = false;
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
                    lowKeys[kidx] = lowCondition.getRight().generateExpression(null);
                    lowInc = (lowCondition.getOperation() == Comparison.GE);
                }
                if (highCondition != null) {
                    highKeys[kidx] = highCondition.getRight().generateExpression(null);
                    highInc = (highCondition.getOperation() == Comparison.LE);
                }
                IndexBound lo = getIndexBound(index, lowKeys);
                IndexBound hi = getIndexBound(index, highKeys);
                return new IndexKeyRange(lo, lowInc, hi, highInc);
            }
        }
    }

    // Pick an index to use.
    protected IndexUsage pickBestIndex(SimplifiedSelectQuery squery) {
        if (squery.getConditions().isEmpty())
            return null;

        IndexUsage bestIndex = null;
        for (UserTable table : squery.getTables()) {
            for (Index index : table.getIndexes()) { // TODO: getIndexesIncludingInternal()
                IndexUsage candidate = new IndexUsage(index);
                if (candidate.usable(squery)) {
                    if ((bestIndex == null) ||
                        (candidate.compareTo(bestIndex) > 0))
                        bestIndex = candidate;
                }
            }
        }
        return bestIndex;
    }

    static class FlattenState {
        private RowType resultRowType;
        private Map<UserTable,Integer> fieldOffsets;
        int nfields;

        public FlattenState(RowType resultRowType,
                            Map<UserTable,Integer> fieldOffsets,
                            int nfields) {
            this.resultRowType = resultRowType;
            this.fieldOffsets = fieldOffsets;
            this.nfields = nfields;
        }
        
        public RowType getResultRowType() {
            return resultRowType;
        }
        public void setResultRowType(RowType resultRowType) {
            this.resultRowType = resultRowType;
        }

        public Map<UserTable,Integer> getFieldOffsets() {
            return fieldOffsets;
        }
        public int getNfields() {
            return nfields;
        }

        
        public void mergeFields(FlattenState other) {
            for (UserTable table : other.fieldOffsets.keySet()) {
                fieldOffsets.put(table, other.fieldOffsets.get(table) + nfields);
            }
            nfields += other.nfields;
        }
    }

    // Holds a partial operator tree while flattening, since need the
    // single return value for above per-branch result.
    class Flattener {
        private PhysicalOperator resultOperator;

        public Flattener(PhysicalOperator resultOperator) {
            this.resultOperator = resultOperator;
        }
        
        public PhysicalOperator getResultOperator() {
            return resultOperator;
        }

        public FlattenState flatten(BaseJoinNode join) {
            if (join.isTable()) {
                UserTable table = ((TableJoinNode)join).getTable();
                Map<UserTable,Integer> fieldOffsets = new HashMap<UserTable,Integer>();
                fieldOffsets.put(table, 0);
                return new FlattenState(userTableRowType(table),
                                        fieldOffsets,
                                        table.getColumns().size());
            }
            else {
                JoinJoinNode jjoin = (JoinJoinNode)join;
                BaseJoinNode left = jjoin.getLeft();
                BaseJoinNode right = jjoin.getRight();
                FlattenState fleft = flatten(left);
                FlattenState fright = flatten(right);
                int flags = 0x00;
                switch (jjoin.getJoinType()) {
                case LEFT:
                    flags = 0x08;
                    break;
                case RIGHT:
                    flags = 0x10;
                    break;
                }
                resultOperator = flatten_HKeyOrdered(resultOperator,
                                                     fleft.getResultRowType(),
                                                     fright.getResultRowType(),
                                                     flags);
                fleft.setResultRowType(resultOperator.rowType());
                fleft.mergeFields(fright);
                return fleft;
            }
        }
    }

    protected UserTableRowType userTableRowType(UserTable table) {
        return schema.userTableRowType(table);
    }

    /** Is t1 an ancestor of t2? */
    protected static boolean isAncestorTable(UserTable t1, UserTable t2) {
        while (true) {
            Join j = t2.getParentJoin();
            if (j == null)
                return false;
            UserTable parent = j.getParent();
            if (parent == null)
                return false;
            if (parent == t1)
                return true;
            t2 = parent;
        }
    }

    protected IndexBound getIndexBound(Index index, Expression[] keys) {
        if (keys == null) 
            return null;
        return new IndexBound((UserTable)index.getTable(), 
                              getIndexExpressionRow(index, keys),
                              getIndexColumnSelector(index));
    }

    protected ColumnSelector getIndexColumnSelector(final Index index) {
        return new ColumnSelector() {
                public boolean includesColumn(int columnPosition) {
                    for (IndexColumn indexColumn : index.getColumns()) {
                        Column column = indexColumn.getColumn();
                        if (column.getPosition() == columnPosition) {
                            return true;
                        }
                    }
                    return false;
                }
            };
    }

    protected Row getIndexExpressionRow(Index index, Expression[] keys) {
        return new ExpressionRow(schema.indexRowType(index), keys);
    }

}
