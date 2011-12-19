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

import com.akiban.sql.optimizer.rule.AggregateMapper.AggregateSourceFinder;

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.AggregateSource.Implementation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Aggregates need to be split into a Project and the aggregation
 * proper, so that the project can go into a nested loop Map. */
public class AggregateSplitter extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(AggregateSplitter.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        List<AggregateSource> sources = new AggregateSourceFinder().find(plan.getPlan());
        for (AggregateSource source : sources) {
            split(source);
        }
    }

    protected void split(AggregateSource source) {
        assert !source.isProjectSplitOff();
        if (!source.hasGroupBy() && source.getAggregates().size() == 1) {
            AggregateFunctionExpression aggr1 = source.getAggregates().get(0);
            if ((aggr1.getOperand() == null) &&
                (aggr1.getFunction().equals("COUNT"))) {
                TableSource singleTable = trivialCountStar(source);
                if (singleTable != null) {
                    source.setImplementation(Implementation.COUNT_TABLE_STATUS);
                    source.setTable(singleTable);
                }
                else
                    // TODO: This is only to support the old
                    // Count_Default operator while we have it.
                    source.setImplementation(Implementation.COUNT_STAR);
                return;
            }
        }
        boolean distinct = checkDistinctDistincts(source);
        // Another way to do this would be to have a different class
        // for AggregateSource in the split-off state. Doing that
        // would require replacing expression references to the old
        // one as a ColumnSource.
        List<ExpressionNode> fields = source.splitOffProject();
        PlanNode input = source.getInput();
        PlanNode ninput = new Project(input, fields);
        if (distinct)
            ninput = new Distinct(ninput);
        source.replaceInput(input, ninput);
    }

    /** Check that all the <code>DISTINCT</code> qualifications are
     * for the same expression and that there are no
     * non-<code>DISTINCT</code> unless they are for the same
     * expression and unaffected by it.
     */
    protected boolean checkDistinctDistincts(AggregateSource source) {
        ExpressionNode operand = null;
        for (AggregateFunctionExpression aggregate : source.getAggregates()) {
            if (aggregate.isDistinct()) {
                ExpressionNode other = aggregate.getOperand();
                if (operand == null)
                    operand = other;
                else if (!operand.equals(other))
                    throw new UnsupportedSQLException("More than one DISTINCT",
                                                      other.getSQLsource());
            }
        }
        if (operand == null)
            return false;
        for (AggregateFunctionExpression aggregate : source.getAggregates()) {
            if (!aggregate.isDistinct()) {
                ExpressionNode other = aggregate.getOperand();
                if (!operand.equals(other))
                    throw new UnsupportedSQLException("Mix of DISTINCT and non-DISTINCT",
                                                      operand.getSQLsource());
                else if (!distinctDoesNotMatter(aggregate.getFunction()))
                    throw new UnsupportedSQLException("Mix of DISTINCT and non-DISTINCT",
                                                      other.getSQLsource());
            }
        }
        return true;
    }

    protected boolean distinctDoesNotMatter(String aggregateFunction) {
        return ("MAX".equals(aggregateFunction) ||
                "MIN".equals(aggregateFunction));
    }

    /** COUNT(*) from single table? */
    protected TableSource trivialCountStar(AggregateSource source) {
        PlanNode input = source.getInput();
        if (!(input instanceof Select))
            return null;
        Select select = (Select)input;
        if (!select.getConditions().isEmpty())
            return null;
        input = select.getInput();
        if (input instanceof IndexScan) {
            IndexScan index = (IndexScan)input;
            if (index.isCovering() && !index.hasConditions() &&
                index.getIndex().isTableIndex())
                return index.getLeafMostTable();
        }
        else if (input instanceof Flatten) {
            Flatten flatten = (Flatten)input;
            if (flatten.getTableNodes().size() != 1)
                return null;
            input = flatten.getInput();
            if (input instanceof GroupScan)
                return flatten.getTableSources().get(0);
        }
        return null;
    }

}
