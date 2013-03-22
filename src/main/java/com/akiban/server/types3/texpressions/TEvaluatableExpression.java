
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.types3.pvalue.PValueSource;

public interface TEvaluatableExpression {

    PValueSource resultValue();

    void evaluate();

    void with(Row row);
    void with(QueryContext context);
}
