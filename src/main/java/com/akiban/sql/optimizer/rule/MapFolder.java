/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;

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
        if (maps.isEmpty()) return;
        if (query instanceof DMLStatement) {
            DMLStatement update = (DMLStatement)query;
            switch (update.getType()) {
            case UPDATE:
            case DELETE:
                addUpdateInput(update);
                break;
            }
        }
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
            {
                PlanNode inner = map.getInner();
                if ((inner instanceof Select) && 
                    ((Select)inner).getConditions().isEmpty())
                    inner = ((Select)inner).getInput();
                // Right-nested semi-joins only need one Limit, since
                // all the effects happen inside the fastest loop.
                if (!((inner instanceof MapJoin) && 
                      ((MapJoin)inner).getJoinType() == JoinNode.JoinType.SEMI))
                    inner = new Limit(map.getInner(), 1);
                map.setInner(inner);
            }
            break;
        case ANTI:
            map.setInner(new OnlyIfEmpty(map.getInner()));
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
    // next pass.
    protected void foldOuterMap(MapJoin map) {
        if (map.getOuter() instanceof MapJoin) {
            MapJoin otherMap = (MapJoin)map.getOuter();
            foldOuterMap(otherMap);
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
                   (parent instanceof Subquery) ||
                   (parent instanceof ResultSet) ||
                   (parent instanceof AggregateSource) ||
                   (parent instanceof Sort) ||
                   // Captures enough at the edge of the inside.
                   (child instanceof Project) ||
                   (child instanceof UpdateInput)));
        if (child != map) {
            map.getOutput().replaceInput(map, map.getInner());
            parent.replaceInput(child, map);
            map.setInner(child);
        }
    }

    protected void addUpdateInput(DMLStatement update) {
        BasePlanWithInput node = update;
        while (true) {
            PlanNode input = node.getInput();
            if (!(input instanceof BasePlanWithInput))
                break;
            node = (BasePlanWithInput)input;
            if (node instanceof BaseUpdateStatement) {
                input = node.getInput();
                node.replaceInput(input, new UpdateInput(input, update.getSelectTable()));
                return;
            }
        }
    }

}
