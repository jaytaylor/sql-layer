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

import com.akiban.qp.operator.Operator;
import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.optimizer.*;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ParameterNode;

import com.akiban.qp.operator.UndefBindings;
import static com.akiban.qp.operator.API.*;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.*;

import static com.akiban.qp.expression.API.*;

import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.ExpressionRow;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.expression.UnboundExpressions;

import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.GroupTable;

import com.akiban.server.api.dml.ColumnSelector;

import java.util.*;

public class OperatorAssembler extends BaseRule
{
    @Override
    public void apply(PlanContext plan) {
        new Assembler(plan).apply();
    }

    static class Assembler {
        private PlanContext planContext;
        private SchemaRulesContext rulesContext;
        private Schema schema;

        public Assembler(PlanContext planContext) {
            this.planContext = planContext;
            rulesContext = (SchemaRulesContext)planContext.getRulesContext();
            schema = rulesContext.getSchema();
        }

        public void apply() {
            planContext.setPlan(assembleStatement((BaseStatement)planContext.getPlan()));
        }
        
        protected BasePlannable assembleStatement(BaseStatement plan) {
            if (plan instanceof SelectQuery)
                return selectQuery((SelectQuery)plan);
            else if (plan instanceof InsertStatement)
                return insertStatement((InsertStatement)plan);
            else if (plan instanceof UpdateStatement)
                return updateStatement((UpdateStatement)plan);
            else if (plan instanceof DeleteStatement)
                return deleteStatement((DeleteStatement)plan);
            else
                throw new UnsupportedSQLException("Cannot assemble plan: " + plan, null);
        }

        protected PhysicalSelect selectQuery(SelectQuery selectQuery) {
            PlanNode planQuery = selectQuery.getQuery();
            RowStream stream = assembleQuery(planQuery);
            List<PhysicalResultColumn> resultColumns;
            if (planQuery instanceof ResultSet) {
                List<ResultField> results = ((ResultSet)planQuery).getFields();
                resultColumns = getResultColumns(results);
            }
            else {
                // VALUES results in column1, column2, ...
                resultColumns = getResultColumns(stream.rowType.nFields());
            }
            return new PhysicalSelect(stream.operator, resultColumns,
                                      getParameterTypes());
        }

        protected PhysicalUpdate insertStatement(InsertStatement insertStatement) {
            PlanNode planQuery = insertStatement.getQuery();
            List<ExpressionNode> projectFields = null;
            if (planQuery instanceof Project) {
                Project project = (Project)planQuery;
                projectFields = project.getFields();
                planQuery = project.getInput();
            }
            RowStream stream = assembleQuery(planQuery);
            UserTableRowType targetRowType = 
                tableRowType(insertStatement.getTargetTable());
            List<Expression> inserts = null;
            if (projectFields != null) {
                // In the common case, we can project into a wider row
                // of the correct type directly.
                inserts = assembleExpressions(projectFields, stream.fieldOffsets);
            }
            else {
                // VALUES just needs each field, which will get rearranged below.
                int nfields = stream.rowType.nFields();
                inserts = new ArrayList<Expression>(nfields);
                for (int i = 0; i < nfields; i++) {
                    inserts.add(field(i));
                }
            }
            // Have a list of expressions in the order specified.
            // Want a list as wide as the target row with NULL
            // literals for the gaps.
            // TODO: That doesn't seem right. How are explicit NULLs
            // to be distinguished from the column's default value?
            Expression[] row = new Expression[targetRowType.nFields()];
            Arrays.fill(row, literal(null));
            int ncols = inserts.size();
            for (int i = 0; i < ncols; i++) {
                Column column = insertStatement.getTargetColumns().get(i);
                row[column.getPosition()] = inserts.get(i);
            }
            inserts = Arrays.asList(row);
            stream.operator = project_Table(stream.operator, stream.rowType,
                                            targetRowType, inserts);
            UpdatePlannable plan = insert_Default(stream.operator);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        protected PhysicalUpdate updateStatement(UpdateStatement updateStatement) {
            RowStream stream = assembleQuery(updateStatement.getQuery());
            UserTableRowType targetRowType = 
                tableRowType(updateStatement.getTargetTable());
            assert (stream.rowType == targetRowType);
            List<UpdateColumn> updateColumns = updateStatement.getUpdateColumns();
            List<Expression> updates = assembleExpressionsA(updateColumns,
                                                            stream.fieldOffsets);
            // Have a list of expressions in the order specified.
            // Want a list as wide as the target row with Java nulls
            // for the gaps.
            // TODO: It might be simpler to have an update function
            // that knew about column offsets for ordered expressions.
            Expression[] row = new Expression[targetRowType.nFields()];
            for (int i = 0; i < updateColumns.size(); i++) {
                UpdateColumn column = updateColumns.get(i);
                row[column.getColumn().getPosition()] = updates.get(i);
            }
            updates = Arrays.asList(row);
            UpdateFunction updateFunction = 
                new ExpressionRowUpdateFunction(updates, targetRowType);
            UpdatePlannable plan = update_Default(stream.operator, updateFunction);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        protected PhysicalUpdate deleteStatement(DeleteStatement deleteStatement) {
            RowStream stream = assembleQuery(deleteStatement.getQuery());
            assert (stream.rowType == tableRowType(deleteStatement.getTargetTable()));
            UpdatePlannable plan = delete_Default(stream.operator);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        // Assemble the top-level query. If there is a ResultSet at
        // the top, it is not handled here, since its meaning is
        // different for the different statement types.
        protected RowStream assembleQuery(PlanNode planQuery) {
            if (planQuery instanceof ResultSet)
                planQuery = ((ResultSet)planQuery).getInput();
            return assembleStream(planQuery);
        }

        // Assemble an ordinary stream node.
        protected RowStream assembleStream(PlanNode node) {
            if (node instanceof IndexScan)
                return assembleIndexScan((IndexScan)node);
            else if (node instanceof GroupScan)
                return assembleGroupScan((GroupScan)node);
            else if (node instanceof Filter)
                return assembleFilter((Filter)node);
            else if (node instanceof Flatten)
                return assembleFlatten((Flatten)node);
            else if (node instanceof AncestorLookup)
                return assembleAncestorLookup((AncestorLookup)node);
            else if (node instanceof BranchLookup)
                return assembleBranchLookup((BranchLookup)node);
            else if (node instanceof AggregateSource)
                return assembleAggregateSource((AggregateSource)node);
            else if (node instanceof Distinct)
                return assembleDistinct((Distinct)node);
            else if (node instanceof Sort            )
                return assembleSort((Sort)node);
            else if (node instanceof Limit)
                return assembleLimit((Limit)node);
            else if (node instanceof Project)
                return assembleProject((Project)node);
            else if (node instanceof ExpressionsSource)
                return assembleExpressionsSource((ExpressionsSource)node);
            else
                throw new UnsupportedSQLException("Plan node " + node, null);
        }

        protected RowStream assembleIndexScan(IndexScan indexScan) {
            RowStream stream = new RowStream();
            IndexRowType indexRowType = schema.indexRowType(indexScan.getIndex());
            stream.operator = indexScan_Default(indexRowType, 
                                                indexScan.isReverseScan(),
                                                assembleIndexKeyRange(indexScan, null),
                                                tableRowType(indexScan.getLeafMostTable()));
            stream.rowType = indexRowType;
            stream.fieldOffsets = indexScan;
            return stream;
        }

        protected RowStream assembleGroupScan(GroupScan groupScan) {
            RowStream stream = new RowStream();
            GroupTable groupTable = groupScan.getGroup().getGroup().getGroupTable();
            stream.operator = groupScan_Default(groupTable);
            stream.unknownTypesPresent = true;
            return stream;
        }

        protected RowStream assembleExpressionsSource(ExpressionsSource expressionsSource) {
            RowStream stream = new RowStream();
            stream.rowType = valuesRowType(expressionsSource.getNFields());
            List<Row> rows = new ArrayList<Row>(expressionsSource.getExpressions().size());
            for (List<ExpressionNode> exprs : expressionsSource.getExpressions()) {
                // TODO: Maybe it would be simpler if ExpressionRow used Lists instead
                // of arrays.
                int nexpr = exprs.size();
                Expression[] expressions = new Expression[nexpr];
                for (int i = 0; i < nexpr; i++) {
                    expressions[i] = assembleExpression(exprs.get(i), 
                                                        stream.fieldOffsets);
                }
                rows.add(new ExpressionRow(stream.rowType, UndefBindings.only(), 
                                           Arrays.asList(expressions)));
            }
            stream.operator = valuesScan_Default(rows, stream.rowType);
            return stream;
        }

        protected RowStream assembleFilter(Filter filter) {
            RowStream stream = assembleStream(filter.getInput());
            for (ConditionExpression condition : filter.getConditions())
                // TODO: Only works for fully flattened; for earlier
                // conditions, need more complex mapping between row
                // types and field offsets.
                stream.operator = select_HKeyOrdered(stream.operator,
                                                     stream.rowType,
                                                     assembleExpression(condition, stream.fieldOffsets));
            return stream;
        }

        protected RowStream assembleFlatten(Flatten flatten) {
            RowStream stream = assembleStream(flatten.getInput());
            Joinable joins = flatten.getJoins();
            if (joins instanceof TableSource) {
                TableSource table = (TableSource)joins;
                stream.rowType = tableRowType(table);
                stream.fieldOffsets = new ColumnSourceFieldOffsets(table);
            }
            else {
                Flattened flattened = flattened(joins, stream);
                stream.rowType = flattened.rowType;
                stream.fieldOffsets = flattened;
            }
            if (stream.unknownTypesPresent) {
                stream.operator = filter_Default(stream.operator,
                                                 Collections.singletonList(stream.rowType));
                stream.unknownTypesPresent = false;
            }
            return stream;
        }

        protected Flattened flattened(Joinable joinable, RowStream stream) {
            if (joinable instanceof TableSource) {
                TableSource table = (TableSource)joinable;
                Flattened f = new Flattened();
                f.rowType = tableRowType(table);
                f.tableOffsets = new HashMap<TableSource,Integer>();
                f.tableOffsets.put(table, 0);
                return f;
            }
            else {
                JoinNode join = (JoinNode)joinable;
                Flattened fleft = flattened(join.getLeft(), stream);
                Flattened fright = flattened(join.getRight(), stream);
                stream.operator = flatten_HKeyOrdered(stream.operator,
                                                      fleft.rowType,
                                                      fright.rowType,
                                                      join.getJoinType());
                int offset = fleft.rowType.nFields();
                fleft.rowType = stream.operator.rowType();
                for (Map.Entry<TableSource,Integer> entry : fright.tableOffsets.entrySet())
                    if (!fleft.tableOffsets.containsKey(entry.getKey()))
                        fleft.tableOffsets.put(entry.getKey(), 
                                               entry.getValue() + offset);
                return fleft;
            }
        }

        static class Flattened implements ColumnExpressionToIndex {
            RowType rowType;
            Map<TableSource,Integer> tableOffsets;

            @Override
            public int getIndex(ColumnExpression column) {
                Integer tableOffset = tableOffsets.get(column.getTable());
                if (tableOffset == null)
                    return -1;
                return tableOffset + column.getPosition();
            }
        }

        protected RowStream assembleAncestorLookup(AncestorLookup ancestorLookup) {
            RowStream stream = assembleStream(ancestorLookup.getInput());
            GroupTable groupTable = ancestorLookup.getDescendant().getTable().getGroup().getGroupTable();
            RowType inputRowType = stream.rowType; // The index row type.
            LookupOption flag = LookupOption.DISCARD_INPUT;
            if (!(inputRowType instanceof IndexRowType)) {
                // Getting from branch lookup.
                inputRowType = tableRowType(ancestorLookup.getDescendant());
                flag = LookupOption.KEEP_INPUT;
            }
            List<RowType> ancestorTypes = 
                new ArrayList<RowType>(ancestorLookup.getAncestors().size());
            for (TableSource table : ancestorLookup.getAncestors()) {
                ancestorTypes.add(tableRowType(table));
            }
            stream.operator = ancestorLookup_Default(stream.operator,
                                                     groupTable,
                                                     inputRowType,
                                                     ancestorTypes,
                                                     flag);
            stream.rowType = null;
            stream.fieldOffsets = null;
            return stream;
        }

        protected RowStream assembleBranchLookup(BranchLookup branchLookup) {
            RowStream stream = assembleStream(branchLookup.getInput());
            RowType inputRowType = stream.rowType; // The index row type.
            LookupOption flag = LookupOption.DISCARD_INPUT;
            if (!(inputRowType instanceof IndexRowType)) {
                // Getting from ancestor lookup.
                inputRowType = tableRowType(branchLookup.getSource());
                flag = LookupOption.KEEP_INPUT;
            }
            GroupTable groupTable = branchLookup.getSource().getTable().getGroup().getGroupTable();
            stream.operator = branchLookup_Default(stream.operator, 
                                                   groupTable, 
                                                   inputRowType,
                                                   tableRowType(branchLookup.getBranch()), 
                                                   flag);
            stream.rowType = null;
            stream.unknownTypesPresent = true;
            stream.fieldOffsets = null;
            return stream;
        }

        protected RowStream assembleAggregateSource(AggregateSource aggregateSource) {
            RowStream stream = assembleStream(aggregateSource.getInput());
            int nkeys = aggregateSource.getGroupBy().size();
            int naggs = aggregateSource.getAggregates().size();
            List<Expression> expressions = new ArrayList<Expression>(nkeys + naggs);
            List<String> aggregatorNames = new ArrayList<String>(naggs);
            for (ExpressionNode groupBy : aggregateSource.getGroupBy()) {
                expressions.add(assembleExpression(groupBy, stream.fieldOffsets));
            }
            for (AggregateFunctionExpression aggr : aggregateSource.getAggregates()) {
                // Should have been split up by now.
                assert !aggr.isDistinct();
                expressions.add(assembleExpression(aggr.getOperand(),
                                                   stream.fieldOffsets));
                aggregatorNames.add(aggr.getFunction());
            }
            stream.operator = project_Default(stream.operator, stream.rowType, 
                                              expressions);
            stream.rowType = stream.operator.rowType();
            if (aggregateSource.getImplementation() != AggregateSource.Implementation.PRESORTED) {
                // TODO: Could pre-aggregate now in PREAGGREGATE_RESORT case.
                Ordering ordering = ordering();
                for (int i = 0; i < nkeys; i++) {
                    ordering.append(field(i), true);
                }
                stream.operator = sort_Tree(stream.operator, stream.rowType, ordering);
            }
            // TODO: Need to get real AggregatorFactory from RulesContext.
            AggregatorFactory aggregatorFactory = new AggregatorFactory() {
                    @Override
                    public Aggregator get(String name) {
                        throw new UnsupportedSQLException(name, null);
                    }
                    @Override
                    public void validateNames(List<String> names) {
                    }
                };
            stream.operator = aggregate_Partial(stream.operator, nkeys, 
                                                aggregatorFactory, aggregatorNames);
            stream.fieldOffsets = new ColumnSourceFieldOffsets(aggregateSource);
            return stream;
        }

        protected RowStream assembleDistinct(Distinct distinct) {
            RowStream stream = assembleStream(distinct.getInput());
            stream.operator = aggregate_Partial(stream.operator,
                                                stream.rowType.nFields(),
                                                null,
                                                Collections.<String>emptyList());
            // TODO: Probably want separate Distinct operator so that
            // row type does not change.
            stream.rowType = stream.operator.rowType();
            stream.fieldOffsets = null;
            return stream;
        }

        static final int INSERTION_SORT_MAX_LIMIT = 100;

        protected RowStream assembleSort(Sort sort) {
            RowStream stream = assembleStream(sort.getInput());
            Ordering ordering = ordering();
            for (OrderByExpression orderBy : sort.getOrderBy()) {
                ordering.append(assembleExpression(orderBy.getExpression(),
                                                   stream.fieldOffsets),
                                orderBy.isAscending());
            }
            int maxrows = -1;
            if (sort.getOutput() instanceof Limit) {
                Limit limit = (Limit)sort.getOutput();
                if (!limit.isOffsetParameter() && !limit.isLimitParameter()) {
                    maxrows = limit.getOffset() + limit.getLimit();
                }
            }
            else {
                // TODO: Also if input is VALUES, whose size we know in advance.
            }
            if ((maxrows >= 0) && (maxrows <= INSERTION_SORT_MAX_LIMIT))
                stream.operator = sort_InsertionLimited(stream.operator, stream.rowType,
                                                        ordering, maxrows);
            else
                stream.operator = sort_Tree(stream.operator, stream.rowType, ordering);
            return stream;
        }

        protected RowStream assembleLimit(Limit limit) {
            RowStream stream = assembleStream(limit.getInput());
            int nlimit = limit.getLimit();
            if ((nlimit < 0) && !limit.isLimitParameter())
                nlimit = Integer.MAX_VALUE; // Slight disagreement in saying unlimited.
            stream.operator = limit_Default(stream.operator, 
                                            limit.getOffset(), limit.isOffsetParameter(),
                                            nlimit, limit.isLimitParameter());
            return stream;
        }

        protected RowStream assembleProject(Project project) {
            RowStream stream = assembleStream(project.getInput());
            stream.operator = project_Default(stream.operator,
                                              stream.rowType,
                                              assembleExpressions(project.getFields(),
                                                                  stream.fieldOffsets));
            stream.rowType = stream.operator.rowType();
            // TODO: If Project were a ColumnSource, could use it to
            // calculate intermediate results and change downstream
            // references to use it instead of expressions. Then could
            // have a straight map of references into projected row.
            stream.fieldOffsets = null;
            return stream;
        }

        // Assemble a list of expressions from the given nodes.
        protected List<Expression> assembleExpressions(List<ExpressionNode> expressions,
                                                       ColumnExpressionToIndex fieldOffsets) {
            List<Expression> result = new ArrayList<Expression>(expressions.size());
            for (ExpressionNode expr : expressions) {
                result.add(assembleExpression(expr, fieldOffsets));
            }
            return result;
        }

        // Assemble a list of expressions from the given nodes.
        protected List<Expression> 
            assembleExpressionsA(List<? extends AnnotatedExpression> expressions,
                                 ColumnExpressionToIndex fieldOffsets) {
            List<Expression> result = new ArrayList<Expression>(expressions.size());
            for (AnnotatedExpression aexpr : expressions) {
                result.add(assembleExpression(aexpr.getExpression(), fieldOffsets));
            }
            return result;
        }

        // Assemble an expression against the given row offsets.
        protected Expression assembleExpression(ExpressionNode expr,
                                                ColumnExpressionToIndex fieldOffsets) {
            return expr.generateExpression(fieldOffsets);
        }

        // Get a list of result columns based on ResultSet expression names.
        protected List<PhysicalResultColumn> getResultColumns(List<ResultField> fields) {
            List<PhysicalResultColumn> columns = 
                new ArrayList<PhysicalResultColumn>(fields.size());
            for (ResultField field : fields) {
                columns.add(rulesContext.getResultColumn(field));
            }
            return columns;
        }

        // Get a list of result columns for unnamed columns.
        // This would correspond to top-level VALUES, which the parser
        // does not currently support.
        protected List<PhysicalResultColumn> getResultColumns(int ncols) {
            List<PhysicalResultColumn> columns = 
                new ArrayList<PhysicalResultColumn>(ncols);
            for (int i = 0; i < ncols; i++) {
                columns.add(rulesContext.getResultColumn(new ResultField("column" + (i+1))));
            }
            return columns;
        }

        // Generate key range bounds.
        protected IndexKeyRange assembleIndexKeyRange(IndexScan index,
                                                      ColumnExpressionToIndex fieldOffsets) {
            List<ExpressionNode> equalityComparands = index.getEqualityComparands();
            ExpressionNode lowComparand = index.getLowComparand();
            ExpressionNode highComparand = index.getHighComparand();
            if ((equalityComparands == null) &&
                (lowComparand == null) && (highComparand == null))
                return new IndexKeyRange(null, false, null, false);

            int nkeys = index.getIndex().getColumns().size();
            Expression[] keys = new Expression[nkeys];

            int kidx = 0;
            if (equalityComparands != null) {
                for (ExpressionNode comp : equalityComparands) {
                    keys[kidx++] = assembleExpression(comp, fieldOffsets);
                }
            }

            if ((lowComparand == null) && (highComparand == null)) {
                IndexBound eq = getIndexBound(index.getIndex(), keys, kidx);
                return new IndexKeyRange(eq, true, eq, true);
            }
            else {
                Expression[] lowKeys = null, highKeys = null;
                boolean lowInc = false, highInc = false;
                int lidx = kidx, hidx = kidx;
                if (lowComparand != null) {
                    lowKeys = keys;
                    if (highComparand != null) {
                        highKeys = new Expression[nkeys];
                        System.arraycopy(keys, 0, highKeys, 0, kidx);
                    }
                }
                else if (highComparand != null) {
                    highKeys = keys;
                }
                if (lowComparand != null) {
                    lowKeys[lidx++] = assembleExpression(lowComparand, fieldOffsets);
                    lowInc = index.isLowInclusive();
                }
                if (highComparand != null) {
                    highKeys[hidx++] = assembleExpression(highComparand, fieldOffsets);
                    highInc = index.isHighInclusive();
                }
                IndexBound lo = getIndexBound(index.getIndex(), lowKeys, lidx);
                IndexBound hi = getIndexBound(index.getIndex(), highKeys, hidx);
                return new IndexKeyRange(lo, lowInc, hi, highInc);
            }
        }

        protected UserTableRowType tableRowType(TableSource table) {
            return tableRowType(table.getTable());
        }

        protected UserTableRowType tableRowType(TableNode table) {
            return schema.userTableRowType(table.getTable());
        }

        protected ValuesRowType valuesRowType(int nfields) {
            return schema.newValuesType(nfields);
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
        protected ColumnSelector getIndexColumnSelector(final Index index, 
                                                        final int nkeys) {
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
        protected UnboundExpressions getIndexExpressionRow(Index index, 
                                                           Expression[] keys) {
            RowType rowType = schema.indexRowType(index);
            return new RowBasedUnboundExpressions(rowType, Arrays.asList(keys));
        }

        // Get the required type for any parameters to the statement.
        protected DataTypeDescriptor[] getParameterTypes() {
            AST ast = ASTStatementLoader.getAST(planContext);
            if (ast == null)
                return null;
            List<ParameterNode> params = ast.getParameters();
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

    // Struct for multiple value return from assembly.
    static class RowStream {
        Operator operator;
        RowType rowType;
        boolean unknownTypesPresent;
        ColumnExpressionToIndex fieldOffsets;
    }

    static class ColumnSourceFieldOffsets implements ColumnExpressionToIndex {
        private ColumnSource source;

        public ColumnSourceFieldOffsets(ColumnSource source) {
            this.source = source;
        }

        @Override
        public int getIndex(ColumnExpression column) {
            if (column.getTable() != source) 
                return -1;
            else
                return column.getPosition();
        }
    }

}
