package com.akiban.cserver.api.dml.scan;

import java.util.List;

public final class CompositePredicate implements Predicate {
    private enum BooleanOperator {
        AND//, OR
    }

    private final BooleanOperator booleanOp;
    private final Predicate[] predicates;

    public static CompositePredicate ofAnds(Predicate... predicates) {
        return new CompositePredicate(BooleanOperator.AND, predicates);
    }

//    public static CompositePredicate ofOrs(Predicate... predicates) {
//        return new CompositePredicate(BooleanOperator.OR, predicates);
//    }

    private CompositePredicate(BooleanOperator booleanOp, Predicate... predicates) {
        this.booleanOp = booleanOp;
        this.predicates = new Predicate[ predicates.length ];
        System.arraycopy(predicates, 0, this.predicates, 0, predicates.length);
    }
}
