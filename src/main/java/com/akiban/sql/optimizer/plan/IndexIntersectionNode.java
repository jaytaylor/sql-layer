
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;

import java.util.List;

public interface IndexIntersectionNode<C, N extends IndexIntersectionNode<C,N>> {
    UserTable getLeafMostUTable();
    List<IndexColumn> getAllColumns();
    void incrementConditionsCounter(ConditionsCounter<? super C> counter);
    boolean isUseful(ConditionsCount<? super C> counter);
    UserTable findCommonAncestor(N other);
    int getPeggedCount();
}
