
package com.akiban.qp.exec;

import com.akiban.qp.operator.Operator;

import com.akiban.server.explain.Explainable;
import java.util.List;
import java.util.Map;

public interface Plannable extends Explainable {
    List<Operator> getInputOperators();

    String getName();

    String describePlan();

    String describePlan(Operator inputOperator);
}
