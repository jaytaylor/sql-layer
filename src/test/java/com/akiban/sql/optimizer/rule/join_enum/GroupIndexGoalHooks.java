
package com.akiban.sql.optimizer.rule.join_enum;

import com.akiban.sql.optimizer.plan.IndexScan;
import com.google.common.base.Function;

public final class GroupIndexGoalHooks {
    public static void hookIntersectedIndexes(Function<? super IndexScan,Void> visitor) {
        GroupIndexGoal.intersectionEnumerationHook = visitor;
    }
    
    public static void unhookIntersectedIndexes() {
        hookIntersectedIndexes(null);
    }
}
