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

import com.akiban.sql.optimizer.plan.*;

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
            PhysicalOperator operator = assembleQuery(planQuery);
            List<PhysicalResultColumn> resultColumns;
            if (planQuery instanceof ResultSet) {
                List<ResultExpression> results = ((ResultSet)planQuery).getResults();
                operator = project_Default(operator, operator.rowType(),
                                           assembleExpressions(results));
                resultColumns = getResultColumns(results);
            }
            else {
                // VALUES results in column1, column2, ...
                resultColumns = getResultColumns(operator.rowType().nFields());
            }
            return new PhysicalSelect(operator, resultColumns, getParameterTypes());
        }

        protected PhysicalUpdate insertStatement(InsertStatement insertStatement) {
            PlanNode planQuery = selectQuery.getQuery();
            PhysicalOperator operator = assembleQuery(planQuery);
            UserTableRowType targetRowType = 
                tableRowType(insertStatement.getTargetTable());
            List<Expression> inserts = null;
            /** TODO: figure this out.
            if (planQuery instanceof ResultSet) {
                projections = assembleExpressions(((ResultSet)planQuery).getResults());
            }
            else {
                // VALUES projects to [Field(1), Field(2), ...].
                projections = assembleExpressions(operator.rowType().nFields());
            }
            **/
            operator = project_Table(operator, operator.rowType(),
                                     targetRowType, inserts);
            UpdatePlannable plan = new Insert_Default(operator);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        protected PhysicalUpdate updateStatement(UpdateStatement updateStatement) {
            PhysicalOperator operator = assembleQuery(deleteStatement.getQuery());
            UserTableRowType targetRowType = 
                tableRowType(insertStatement.getTargetTable());
            List<Expression> updates = null;
            // TODO: figure this out.
            UpdateFunction updateFunction = 
                new ExpressionRowUpdateFunction(updates, targetRowType);
            UpdatePlannable plan = new Update_Default(operator, updateFunction);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        protected PhysicalUpdate deleteStatement(DeleteStatement deleteStatement) {
            PhysicalOperator operator = assembleQuery(deleteStatement.getQuery());
            UpdatePlannable plan = new Delete_Default(operator);
            return new PhysicalUpdate(plan, getParameterTypes());
        }

        protected UserTableRowType tableRowType(TableNode table) {
            return schema.userTableRowType(table.getTable());
        }

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
        
        protected PhysicalOperator assembleQuery(PlanNode planQuery) {
            return null;
        }

        protected List<Expression> assembleExpressions(List<? extends AnnotatedExpression> expressions) {
            List<Expression> result = new ArrayList<Expression>(expressions.size());
            for (AnnotatedExpression aexpr : expressions) {
                result.add(assembleExpression(aexpr.getExpression()));
            }
            return result;
        }

        // TODO: Need field offsets.
        protected Expression assembleExpression(ExpressionNode expr) {
            return null;
        }

        protected List<PhysicalResultColumn> getResultColumns(List<ResultExpression> results) {
            List<PhysicalResultColumn> result = new ArrayList<PhysicalResultColumn>(results.size());
            // TODO: ...
            return result;
        }

    }
}
