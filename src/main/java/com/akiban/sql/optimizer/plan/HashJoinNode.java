
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
