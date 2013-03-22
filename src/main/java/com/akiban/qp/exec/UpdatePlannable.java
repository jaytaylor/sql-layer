
package com.akiban.qp.exec;

import com.akiban.qp.operator.QueryContext;

public interface UpdatePlannable extends Plannable {
    UpdateResult run(QueryContext context);
}
