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

import com.akiban.sql.optimizer.plan.*;

import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import com.akiban.server.error.UnsupportedSQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Convert nested loop join into map.
 * This rule only does the immediate conversion to a Map node and
 * recording of bound tables. The map still needs to be folded after
 * conditions have moved down and so on.
 */
public class NestedLoopMapper extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(NestedLoopMapper.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class NestedLoopsJoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<JoinNode> result = new ArrayList<JoinNode>();

        public List<JoinNode> find(PlanNode root) {
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
            if (n instanceof JoinNode) {
                JoinNode j = (JoinNode)n;
                if (j.getImplementation() == JoinNode.Implementation.NESTED_LOOPS)
                    result.add(j);
            }
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

    @Override
    public void apply(PlanContext planContext) {
        BaseQuery query = (BaseQuery)planContext.getPlan();
        List<JoinNode> joins = new NestedLoopsJoinsFinder().find(query);
        List<MapJoin> maps = new ArrayList<MapJoin>(joins.size());
        for (JoinNode join : joins) {
            MapJoin map = new MapJoin(join.getJoinType(), 
                                      join.getLeft(), join.getRight());
            if (join.hasJoinConditions())
                map.setInner(new Select(map.getInner(), join.getJoinConditions()));
            join.getOutput().replaceInput(join, map);
            maps.add(map);
        }
        for (MapJoin map : maps) {
            map.setOuterTables(getBoundTables(map.getOuter()));
        }
    }

    protected Set<ColumnSource> getBoundTables(PlanNode node) {
        if (node instanceof TableJoins)
            return new HashSet<ColumnSource>(((TableJoins)node).getTables());
        else if (node instanceof ColumnSource) {
            Set<ColumnSource> single = new HashSet<ColumnSource>(1);
            single.add((ColumnSource)node);
            return single;
        }
        else if (node instanceof MapJoin) {
            MapJoin map = (MapJoin)node;
            Set<ColumnSource> combined = getBoundTables(map.getOuter());
            combined.addAll(getBoundTables(map.getInner()));
            return combined;
        }
        else if (node instanceof Select) {
            // Might be the Select we just added above with join conditions.
            return getBoundTables(((Select)node).getInput());
        }
        else {
            assert false : "Unknown map join input";
            return Collections.<ColumnSource>emptySet();
        }
    }

}
