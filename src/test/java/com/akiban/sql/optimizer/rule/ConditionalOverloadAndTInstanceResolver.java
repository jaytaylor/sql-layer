
package com.akiban.sql.optimizer.rule;

import com.akiban.server.types3.Types3Switch;
import com.akiban.sql.optimizer.rule.PlanContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConditionalOverloadAndTInstanceResolver extends BaseRule {

    private static final Logger logger = LoggerFactory.getLogger(ConditionalOverloadAndTInstanceResolver.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        if (delegate != null)
            delegate.apply(plan);
    }

    public ConditionalOverloadAndTInstanceResolver() {
        delegate = Types3Switch.ON
                ? new OverloadAndTInstanceResolver()
                : null;
    }

    private final BaseRule delegate;
}
