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

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.optimizer.*;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.ResultSet.ResultExpression;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ParameterNode;

import com.akiban.qp.physicaloperator.PhysicalOperator;
import static com.akiban.qp.physicaloperator.API.*;
// TODO: Why aren't these in API?
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.physicaloperator.UpdateFunction;
import com.akiban.qp.physicaloperator.Insert_Default;
import com.akiban.qp.physicaloperator.Update_Default;
import com.akiban.qp.physicaloperator.Delete_Default;
import com.akiban.qp.rowtype.*;

import static com.akiban.qp.expression.API.literal;
import com.akiban.qp.expression.Comparison;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;

import com.akiban.ais.model.Column;
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
        private Schema schema;

        public Assembler(PlanContext planContext) {
            this.planContext = planContext;
            schema = ((SchemaRulesContext)planContext.getRulesContext()).getSchema();
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
                List<ResultExpression> results = ((ResultSet)planQuery).getResults();
                stream.operator = 
                    project_Default(stream.operator, stream.rowType,
                                    assembleExpressions(results, stream.fieldOffsets));
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
            RowStream stream = assembleQuery(planQuery);
            UserTableRowType targetRowType = 
                tableRowType(insertStatement.getTargetTable());
            List<Expression> inserts = null;
            if (planQuery instanceof ResultSet) {
                inserts = assembleExpressions(((ResultSet)planQuery).getResults(),
                                              stream.fieldOffsets);
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
            UpdatePlannable plan = new Insert_Default(stream.operator);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        protected PhysicalUpdate updateStatement(UpdateStatement updateStatement) {
            RowStream stream = assembleQuery(updateStatement.getQuery());
            UserTableRowType targetRowType = 
                tableRowType(updateStatement.getTargetTable());
            assert (stream.rowType == targetRowType);
            List<UpdateColumn> updateColumns = updateStatement.getUpdateColumns();
            List<Expression> updates = assembleExpressions(updateColumns,
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
            UpdatePlannable plan = new Update_Default(stream.operator, updateFunction);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        protected PhysicalUpdate deleteStatement(DeleteStatement deleteStatement) {
            RowStream stream = assembleQuery(deleteStatement.getQuery());
            assert (stream.rowType == tableRowType(deleteStatement.getTargetTable()));
            UpdatePlannable plan = new Delete_Default(stream.operator);
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
            else if (node instanceof ResultSet)
                return assembleResultSet((ResultSet)node);
            else
                throw new UnsupportedSQLException("Plan node " + node, null);
        }

        protected RowStream assembleIndexScan(IndexScan indexScan) {
            return null;
        }

        protected RowStream assembleGroupScan(GroupScan groupScan) {
            GroupTable groupTable = groupScan.getGroup().getGroup().getGroupTable();
            RowStream result = new RowStream();
            result.operator = groupScan_Default(groupTable);
            return result;
        }

        protected RowStream assembleFilter(Filter filter) {
            RowStream stream = assembleStream(filter.getInput());
            for (ConditionExpression condition : filter.getConditions())
                // TODO: Only works for fully flattened; for earlier
                // conditions, need more complex mapping between row
                // types and field offsets.
                stream.operator = select_HKeyOrdered(stream.operator,
                                                     stream.rowType,
                                                     condition.generateExpression(stream.fieldOffsets));
            return stream;
        }

        protected RowStream assembleFlatten(Flatten flatten) {
            RowStream stream = assembleStream(flatten.getInput());
            return stream;
        }

        protected RowStream assembleAncestorLookup(AncestorLookup ancestorLookup) {
            RowStream stream = assembleStream(ancestorLookup.getInput());
            return stream;
        }

        protected RowStream assembleBranchLookup(BranchLookup branchLookup) {
            RowStream stream = assembleStream(branchLookup.getInput());
            return stream;
        }

        protected RowStream assembleAggregateSource(AggregateSource aggregateSource) {
            RowStream stream = assembleStream(aggregateSource.getInput());
            return stream;
        }

        protected RowStream assembleDistinct(Distinct distinct) {
            RowStream stream = assembleStream(distinct.getInput());
            return stream;
        }

        protected RowStream assembleSort(Sort sort) {
            RowStream stream = assembleStream(sort.getInput());
            return stream;
        }

        protected RowStream assembleLimit(Limit limit) {
            RowStream stream = assembleStream(limit.getInput());
            return stream;
        }

        protected RowStream assembleResultSet(ResultSet resultSet) {
            RowStream stream = assembleStream(resultSet.getInput());
            return stream;
        }

        // Assemble a list of expressions from the given nodes.
        protected List<Expression> 
            assembleExpressions(List<? extends AnnotatedExpression> expressions,
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
        protected List<PhysicalResultColumn> 
            getResultColumns(List<ResultExpression> results) {
            List<PhysicalResultColumn> result = 
                new ArrayList<PhysicalResultColumn>(results.size());
            // TODO: ...
            return result;
        }

        // Get a list of result columns for unnamed columns.
        // This would correspond to top-level VALUES, which the parser
        // does not currently support.
        protected List<PhysicalResultColumn> getResultColumns(int ncols) {
            List<PhysicalResultColumn> result = 
                new ArrayList<PhysicalResultColumn>(ncols);
            // TODO: ...
            return result;
        }

        protected UserTableRowType tableRowType(TableNode table) {
            return schema.userTableRowType(table.getTable());
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
        PhysicalOperator operator;
        RowType rowType;
        ColumnExpressionToIndex fieldOffsets;
    }

}
