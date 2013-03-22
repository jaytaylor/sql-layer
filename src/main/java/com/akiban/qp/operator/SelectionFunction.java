
package com.akiban.qp.operator;

import com.akiban.qp.row.Row;

public interface SelectionFunction {
    boolean rowIsSelected(Row row);
}
