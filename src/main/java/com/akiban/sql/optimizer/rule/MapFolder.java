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

/** Take a map join node and push enough into the inner loop that the
 * bindings can be evaluated properly. 
 * Or, looked at another way, what is before expressed through
 * data-flow is after expressed as control-flow.
 */
public class MapFolder extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(MapFolder.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class MapJoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<MapJoin> result = new ArrayList<MapJoin>();

        public List<MapJoin> find(PlanNode root) {
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
            if (n instanceof MapJoin) {
                result.add((MapJoin)n);
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
        List<MapJoin> maps = new MapJoinsFinder().find(query);
        for (MapJoin map : maps)
            handleJoinType(map);
        for (MapJoin map : maps)
            foldOuterMap(map);
        for (MapJoin map : maps)
            fold(map);
    }

    // First pass: account for the join type by adding something at
    // the tip of the inner (fast) side of the loop.
    protected void handleJoinType(MapJoin map) {
        switch (map.getJoinType()) {
        case INNER:
            break;
        case LEFT:
            map.setInner(new NullIfEmpty(map.getInner()));
            break;
        case SEMI:
            map.setInner(new Limit(map.getInner(), 1));
            break;
        default:
            throw new UnsupportedSQLException("complex join type " + map, null);
        }
        map.setJoinType(null);  // No longer special.
    }

    // Second pass: if one map has another on the outer (slow) side,
    // turn them inside out. Nesting must all be on the inner side to
    // be like regular loops. Conceptually, the two trace places, but
    // actually doing that would mess up the depth nesting for the
    // next pass. Which is also why a loop is needed.
    protected void foldOuterMap(MapJoin map) {
        while (map.getOuter() instanceof MapJoin) {
            MapJoin otherMap = (MapJoin)map.getOuter();
            PlanNode inner = map.getInner();
            PlanNode outer = otherMap.getInner();
            map.setOuter(otherMap.getOuter());
            map.setInner(otherMap);
            otherMap.setOuter(outer);
            otherMap.setInner(inner);
        }
    }    

    // Final pass: move things upstream of the map down into the inner (fast) side.
    protected void fold(MapJoin map) {
        PlanWithInput parent = map;
        PlanNode child;
        do {
            child = parent;
            parent = child.getOutput();
        } while (!((parent instanceof MapJoin) ||
                   // These need to be outside.
                   (parent instanceof ResultSet) ||
                   (parent instanceof AggregateSource) ||
                   (parent instanceof Sort) ||
                   // Captures enough at the edge of the inside.
                   (child instanceof Project)));
        if (child != map) {
            map.getOutput().replaceInput(map, map.getInner());
            parent.replaceInput(child, map);
            map.setInner(child);
        }
    }

}
