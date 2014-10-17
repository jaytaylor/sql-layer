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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.server.error.InvalidOptimizerPropertyException;
import com.foundationdb.server.error.NoAggregateWithGroupByException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.TableIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Resolve aggregate functions and group by expressions to output
 * columns of the "group table," that is, the result of aggregation.
 */
public class AggregateMapper extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(AggregateMapper.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        List<AggregateSourceState> sources = new AggregateSourceFinder(plan).find();
        for (AggregateSourceState source : sources) {
            Mapper m = new Mapper((SchemaRulesContext)plan.getRulesContext(), source.aggregateSource, source.containingQuery);
            m.remap(source.aggregateSource);
        }
    }

    static class AggregateSourceFinder extends SubqueryBoundTablesTracker {
        List<AggregateSourceState> result = new ArrayList<>();

        public AggregateSourceFinder(PlanContext planContext) {
            super(planContext);
        }

        public List<AggregateSourceState> find() {
            run();
            return result;
        }

        @Override
        public boolean visit(PlanNode n) {
            super.visit(n);
            if (n instanceof AggregateSource)
                result.add(new AggregateSourceState((AggregateSource)n, currentQuery()));
            return true;
        }
    }

    static class AggregateSourceState {
        AggregateSource aggregateSource;
        BaseQuery containingQuery;

        public AggregateSourceState(AggregateSource aggregateSource, 
                                    BaseQuery containingQuery) {
            this.aggregateSource = aggregateSource;
            this.containingQuery = containingQuery;
        }
    }

    static class Mapper implements ExpressionRewriteVisitor, PlanVisitor {
        private SchemaRulesContext rulesContext;
        private AggregateSource source;
        private BaseQuery query;
        private Deque<BaseQuery> subqueries = new ArrayDeque<>();
        private Set<ColumnSource> aggregated = new HashSet<>();
        private Map<ExpressionNode,ExpressionNode> map = 
            new HashMap<>();
        private enum ImplicitAggregateSetting {
            ERROR, FIRST, FIRST_IF_UNIQUE
        };
        private ImplicitAggregateSetting implicitAggregateSetting;
        private Set<TableSource> uniqueGroupedTables;

        protected ImplicitAggregateSetting getImplicitAggregateSetting() {
            if (implicitAggregateSetting == null) {
                String setting = rulesContext.getProperty("implicitAggregate", "error");
                if ("error".equals(setting))
                    implicitAggregateSetting = ImplicitAggregateSetting.ERROR;
                else if ("first".equals(setting))
                    implicitAggregateSetting = ImplicitAggregateSetting.FIRST;
                else if ("firstIfUnique".equals(setting))
                    implicitAggregateSetting = ImplicitAggregateSetting.FIRST_IF_UNIQUE;
                else
                    throw new InvalidOptimizerPropertyException("implicitAggregate", setting);
            }
            return implicitAggregateSetting;
        }

        public Mapper(SchemaRulesContext rulesContext, AggregateSource source, BaseQuery query) {
            this.rulesContext = rulesContext;
            this.source = source;
            this.query = query;
            aggregated.add(source);
            // Map all the group by expressions at the start.
            // This means that if you GROUP BY x+1, you can ORDER BY
            // x+1, or x+1+1, but not x+2. Postgres is like that, too.
            List<ExpressionNode> groupBy = source.getGroupBy();
            for (int i = 0; i < groupBy.size(); i++) {
                ExpressionNode expr = groupBy.get(i);
                map.put(expr, new ColumnExpression(source, i, 
                                                   expr.getSQLtype(), expr.getSQLsource(), expr.getType()));
            }
        }

        public void remap(PlanNode n) {
            while (true) {
                // Keep going as long as we're feeding something we understand.
                n = n.getOutput();
                if (n instanceof Select) {
                    remap(((Select)n).getConditions());
                }
                else if (n instanceof Sort) {
                    remapA(((Sort)n).getOrderBy());
                }
                else if (n instanceof Project) {
                    Project p = (Project)n;
                    remap(p.getFields());
                    aggregated.add(p);
                }
                else if (n instanceof Limit) {
                    // Understood not but mapped.
                }
                else
                    break;
            }
        }

        @SuppressWarnings("unchecked")
        protected <T extends ExpressionNode> void remap(List<T> exprs) {
            for (int i = 0; i < exprs.size(); i++) {
                exprs.set(i, (T)exprs.get(i).accept(this));
            }
        }

        protected void remapA(List<? extends AnnotatedExpression> exprs) {
            for (AnnotatedExpression expr : exprs) {
                expr.setExpression(expr.getExpression().accept(this));
            }
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode expr) {
            return false;
        }

        @Override
        public ExpressionNode visit(ExpressionNode expr) {
            ExpressionNode nexpr = map.get(expr);
            if (nexpr != null)
                return nexpr;
            if (expr instanceof AggregateFunctionExpression) {
                return addAggregate((AggregateFunctionExpression)expr);
            }
            if (expr instanceof ColumnExpression) {
                ColumnExpression column = (ColumnExpression)expr;
                ColumnSource table = column.getTable();
                if (table instanceof AggregateSource) {
                    ExpressionNode returnNode = null;
                    for (AggregateFunctionExpression afe : ((AggregateSource)table).getAggregates()) {
                        returnNode = addAggregate(afe);
                    }
                    return returnNode;
                }
                if (!aggregated.contains(table) &&
                    !boundElsewhere(table)) {
                    return nonAggregate(column);
                }
            }
            return expr;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (n instanceof BaseQuery)
                subqueries.push((BaseQuery)n);
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof BaseQuery)
                subqueries.pop();
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        protected ExpressionNode addAggregate(AggregateFunctionExpression expr) {
            ExpressionNode nexpr = rewrite(expr);
            if (nexpr != null)
                return nexpr.accept(this);
            int position = source.addAggregate(expr);
            nexpr = new ColumnExpression(source, position,
                                         expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            map.put(expr, nexpr);
            return nexpr;
        }

        // Rewrite agregate functions that aren't well behaved wrt pre-aggregation.
        protected ExpressionNode rewrite(AggregateFunctionExpression expr) {
            String function = expr.getFunction().toUpperCase();
            if ("AVG".equals(function)) {
                ExpressionNode operand = expr.getOperand();
                List<ExpressionNode> noperands = new ArrayList<>(2);
                noperands.add(new AggregateFunctionExpression("SUM", operand, expr.isDistinct(),
                                                              operand.getSQLtype(), null, 
                                                              operand.getType(), null, null));
                DataTypeDescriptor intType = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
                TInstance intInst = rulesContext.getTypesTranslator().typeForSQLType(intType);
                noperands.add(new AggregateFunctionExpression("COUNT", operand, expr.isDistinct(),
                                                              intType, null, intInst, null, null));
                return new FunctionExpression("divide",
                                              noperands,
                                              expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            }
            if ("VAR_POP".equals(function) ||
                "VAR_SAMP".equals(function) ||
                "STDDEV_POP".equals(function) ||
                "STDDEV_SAMP".equals(function)) {
                ExpressionNode operand = expr.getOperand();
                List<ExpressionNode> noperands = new ArrayList<>(3);
                noperands.add(new AggregateFunctionExpression("_VAR_SUM_2", operand, expr.isDistinct(),
                                                              operand.getSQLtype(), null,
                                                              operand.getType(), null, null));
                noperands.add(new AggregateFunctionExpression("_VAR_SUM", operand, expr.isDistinct(),
                                                              operand.getSQLtype(), null,
                                                              operand.getType(), null, null));
                DataTypeDescriptor intType = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
                TInstance intInst = rulesContext.getTypesTranslator().typeForSQLType(intType);
                noperands.add(new AggregateFunctionExpression("COUNT", operand, expr.isDistinct(),
                                                              intType, null, intInst, null, null));
                return new FunctionExpression("_" + function,
                                              noperands,
                                              expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            }
            return null;
        }

        protected ExpressionNode addKey(ExpressionNode expr) {
            int position = source.addGroupBy(expr);
            ColumnExpression nexpr = new ColumnExpression(source, position,
                                                          expr.getSQLtype(), expr.getSQLsource(), expr.getType());
            map.put(expr, nexpr);
            return nexpr;
        }

        protected boolean boundElsewhere(ColumnSource table) {
            if (query.getOuterTables().contains(table))
                return true;    // Bound outside.
            BaseQuery subquery = subqueries.peek();
            if (subquery != null) {
                if (!subquery.getOuterTables().contains(table))
                    return true; // Must be introduced by subquery.
            }
            return false;
        }

        // Use of a column not in GROUP BY without aggregate function.
        protected ExpressionNode nonAggregate(ColumnExpression column) {
            boolean isUnique = isUniqueGroupedTable(column.getTable());
            ImplicitAggregateSetting setting = getImplicitAggregateSetting();
            if ((setting == ImplicitAggregateSetting.ERROR) ||
                ((setting == ImplicitAggregateSetting.FIRST_IF_UNIQUE) && !isUnique))
                throw new NoAggregateWithGroupByException(column.getSQLsource());
            if (isUnique && source.getAggregates().isEmpty())
                // Add unique as another key in hopes of turning the
                // whole things into a distinct.
                return addKey(column);
            else
                return addAggregate(new AggregateFunctionExpression("FIRST", column, false,
                                                                    column.getSQLtype(), null, column.getType(), null, null));
        }

        protected boolean isUniqueGroupedTable(ColumnSource columnSource) {
            if (!(columnSource instanceof TableSource))
                return false;
            TableSource table = (TableSource)columnSource;
            if (uniqueGroupedTables == null)
                uniqueGroupedTables = new HashSet<>();
            if (uniqueGroupedTables.contains(table))
                return true;
            Set<Column> columns = new HashSet<>();
            for (ExpressionNode groupBy : source.getGroupBy()) {
                if (groupBy instanceof ColumnExpression) {
                    ColumnExpression groupColumn = (ColumnExpression)groupBy;
                    if (groupColumn.getTable() == table) {
                        columns.add(groupColumn.getColumn());
                    }
                }
            }
            if (columns.isEmpty()) return false;
            // Find a unique index all of whose columns are in the GROUP BY.
            // TODO: Use column equivalences.
            find_index:
            for (TableIndex index : table.getTable().getTable().getIndexes()) {
                if (!index.isUnique()) continue;
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    if (!columns.contains(indexColumn.getColumn())) {
                        continue find_index;
                    }
                }
                uniqueGroupedTables.add(table);
                return true;
            }
            return false;
        }
    }

}
