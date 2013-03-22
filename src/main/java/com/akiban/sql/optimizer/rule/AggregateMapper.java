/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.server.error.InvalidOptimizerPropertyException;
import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;

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
        List<AggregateSource> sources = new AggregateSourceFinder().find(plan.getPlan());
        for (AggregateSource source : sources) {
            Mapper m = new Mapper(plan.getRulesContext(), source);
            m.remap(source);
        }
    }

    static class AggregateSourceFinder implements PlanVisitor, ExpressionVisitor {
        List<AggregateSource> result = new ArrayList<>();

        public List<AggregateSource> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof AggregateSource)
                result.add((AggregateSource)n);
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }
        @Override
        public boolean visit(ExpressionNode n) {
            return true;
        }
    }

    static class Mapper implements ExpressionRewriteVisitor {
        private RulesContext rulesContext;
        private AggregateSource source;
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

        public Mapper(RulesContext rulesContext, AggregateSource source) {
            this.rulesContext = rulesContext;
            this.source = source;
            aggregated.add(source);
            // Map all the group by expressions at the start.
            // This means that if you GROUP BY x+1, you can ORDER BY
            // x+1, or x+1+1, but not x+2. Postgres is like that, too.
            List<ExpressionNode> groupBy = source.getGroupBy();
            for (int i = 0; i < groupBy.size(); i++) {
                ExpressionNode expr = groupBy.get(i);
                map.put(expr, new ColumnExpression(source, i, 
                                                   expr.getSQLtype(), expr.getSQLsource()));
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
                if (!aggregated.contains(column.getTable())) {
                    return nonAggregate(column);
                }
            }
            return expr;
        }

        protected ExpressionNode addAggregate(AggregateFunctionExpression expr) {
            ExpressionNode nexpr = rewrite(expr);
            if (nexpr != null)
                return nexpr.accept(this);
            int position = source.addAggregate(expr);
            nexpr = new ColumnExpression(source, position,
                                         expr.getSQLtype(), expr.getAkType(), expr.getSQLsource());
            map.put(expr, nexpr);
            return nexpr;
        }

        // Rewrite agregate functions that aren't well behaved wrt pre-aggregation.
        protected ExpressionNode rewrite(AggregateFunctionExpression expr) {
            String function = expr.getFunction();
            if ("AVG".equals(function)) {
                ExpressionNode operand = expr.getOperand();
                List<ExpressionNode> noperands = new ArrayList<>(2);
                noperands.add(new AggregateFunctionExpression("SUM", operand, expr.isDistinct(),
                                                              operand.getSQLtype(), null, null, null));
                noperands.add(new AggregateFunctionExpression("COUNT", operand, expr.isDistinct(),
                                                              new DataTypeDescriptor(TypeId.INTEGER_ID, false), null, null, null));
                return new FunctionExpression("divide",
                                              noperands,
                                              expr.getSQLtype(), expr.getSQLsource());
            }
            // TODO: {VAR,STDDEV}_{POP,SAMP}
            return null;
        }

        protected ExpressionNode addKey(ExpressionNode expr) {
            int position = source.addGroupBy(expr);
            ColumnExpression nexpr = new ColumnExpression(source, position,
                                                          expr.getSQLtype(), expr.getAkType(), expr.getSQLsource());
            map.put(expr, nexpr);
            return nexpr;
        }

        // Use of a column not in GROUP BY without aggregate function.
        protected ExpressionNode nonAggregate(ColumnExpression column) {
            boolean isUnique = isUniqueGroupedTable(column.getTable());
            ImplicitAggregateSetting setting = getImplicitAggregateSetting();
            if ((setting == ImplicitAggregateSetting.ERROR) ||
                ((setting == ImplicitAggregateSetting.FIRST_IF_UNIQUE) && !isUnique))
                throw new UnsupportedSQLException("Column cannot be used outside aggregate function or GROUP BY", column.getSQLsource());
            if (isUnique && source.getAggregates().isEmpty())
                // Add unique as another key in hopes of turning the
                // whole things into a distinct.
                return addKey(column);
            else
                return addAggregate(new AggregateFunctionExpression("FIRST", column, false,
                                                                    column.getSQLtype(), null, null, null));
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
