
package com.akiban.sql.optimizer.plan;

public final class OverlayedConditionsCount<C> implements ConditionsCount<C> {

    @Override
    public HowMany getCount(C condition) {
        HowMany oneCount = one.getCount(condition);
        switch (oneCount) {
        case NONE:
            return two.getCount(condition);
        case ONE:
            return two.getCount(condition) == HowMany.NONE ? HowMany.ONE : HowMany.MANY;
        case MANY:
            return HowMany.MANY;
        default:
            throw new AssertionError(oneCount.name());
        }
    }

    public OverlayedConditionsCount(ConditionsCount<? super C> one, ConditionsCount<? super C> two) {
        this.one = one;
        this.two = two;
    }

    private ConditionsCount<? super C> one;
    private ConditionsCount<? super C> two;
}
