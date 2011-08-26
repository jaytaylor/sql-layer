/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.expression.std;

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.ExpressionEvaluation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractCompositeExpressionEvaluation implements ExpressionEvaluation {
    @Override
    public void of(Row row, Bindings bindings) {
        for (ExpressionEvaluation child : children) {
            child.of(row, bindings);
        }
    }

    protected final List<? extends ExpressionEvaluation> children() {
        return children;
    }

    public AbstractCompositeExpressionEvaluation(List<? extends ExpressionEvaluation> children) {
        this.children = children.isEmpty()
                ? Collections.<ExpressionEvaluation>emptyList()
                : Collections.unmodifiableList(new ArrayList<ExpressionEvaluation>(children));
    }

    private final List<? extends ExpressionEvaluation> children;
}
