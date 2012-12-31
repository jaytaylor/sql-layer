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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Convert nested loop join into map.
 * This rule only does the immediate conversion to a Map. The map
 * still needs to be folded after conditions have moved down and so on.
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
                switch (j.getImplementation()) {
                case NESTED_LOOPS:
                case BLOOM_FILTER:
                    result.add(j);
                }
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
        for (JoinNode join : joins) {
            PlanNode outer = join.getLeft();
            PlanNode inner = join.getRight();
            if (join.hasJoinConditions())
                inner = new Select(inner, join.getJoinConditions());
            PlanNode map;
            switch (join.getImplementation()) {
            case NESTED_LOOPS:
                map = new MapJoin(join.getJoinType(), outer, inner);
                break;
            case BLOOM_FILTER:
                {
                    HashJoinNode hjoin = (HashJoinNode)join;
                    BloomFilter bf = (BloomFilter)hjoin.getHashTable();
                    map = new BloomFilterFilter(bf, hjoin.getMatchColumns(),
                                                outer, inner);
                    PlanNode loader = hjoin.getLoader();
                    loader = new Project(loader, hjoin.getHashColumns());
                    map = new UsingBloomFilter(bf, loader, map);
                }
                break;
            default:
                assert false : join;
                map = join;
            }
            join.getOutput().replaceInput(join, map);
        }
    }

}
