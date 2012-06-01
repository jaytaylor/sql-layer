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

package com.akiban.sql.optimizer.plan;

import java.util.List;

/** A join with some kind of hash table loading. */
public class HashJoinNode extends JoinNode
{
    private Joinable loader;
    private BaseHashTable hashTable;
    private List<ExpressionNode> hashColumns, matchColumns;

    public HashJoinNode(Joinable loader, Joinable input, Joinable check, JoinType joinType, 
                        BaseHashTable hashTable, List<ExpressionNode> hashColumns, List<ExpressionNode> matchColumns) {
        super(input, check, joinType);
        this.loader = loader;
        loader.setOutput(this);
        this.hashTable = hashTable;
        this.hashColumns = hashColumns;
        this.matchColumns = matchColumns;
    }

    public Joinable getLoader() {
        return loader;
    }
    public Joinable getInput() {
        return getLeft();
    }
    public Joinable getCheck() {
        return getRight();
    }

    public BaseHashTable getHashTable() {
        return hashTable;
    }
    public List<ExpressionNode> getHashColumns() {
        return hashColumns;
    }
    public List<ExpressionNode> getMatchColumns() {
        return matchColumns;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (loader == oldInput) {
            loader = (Joinable)newInput;
            loader.setOutput(this);
        }
        super.replaceInput(oldInput, newInput);
    }

    @Override
    protected boolean acceptPlans(PlanVisitor v) {
        return (loader.accept(v) && super.acceptPlans(v));
    }

    @Override
    protected void acceptConditions(PlanVisitor v) {
        super.acceptConditions(v);
        if (v instanceof ExpressionRewriteVisitor) {
            for (int i = 0; i < hashColumns.size(); i++) {
                hashColumns.set(i, hashColumns.get(i).accept((ExpressionRewriteVisitor)v));
                matchColumns.set(i, matchColumns.get(i).accept((ExpressionRewriteVisitor)v));
            }
        }
        else if (v instanceof ExpressionVisitor) {
            for (int i = 0; i < hashColumns.size(); i++) {
                if (!hashColumns.get(i).accept((ExpressionVisitor)v))
                    break;
                if (!matchColumns.get(i).accept((ExpressionVisitor)v))
                    break;
            }
        }
    }

    @Override
    protected void summarizeJoins(StringBuilder str) {
        super.summarizeJoins(str);
        str.append(hashColumns);
        str.append(" = ");
        str.append(matchColumns);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        loader = (Joinable)loader.duplicate(map);
        hashColumns = duplicateList(hashColumns, map);
        matchColumns = duplicateList(matchColumns, map);
    }

}
