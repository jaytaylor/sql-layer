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

package com.akiban.server.types3.playground;

import com.akiban.qp.row.Row;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public final class XPreparedField implements XPreparedExpression {
    @Override
    public TPreptimeValue evaluateConstant() {
        return null;
    }

    @Override
    public TInstance resultType() {
        return typeInstance;
    }

    @Override
    public XEvaluatableExpression build() {
        return new Evaluation(typeInstance.typeClass().underlyingType(), fieldIndex);
    }

    public XPreparedField(TInstance typeInstance, int fieldIndex) {
        this.typeInstance = typeInstance;
        this.fieldIndex = fieldIndex;
    }

    private final TInstance typeInstance;
    private final int fieldIndex;
    
    private static class Evaluation extends ContextualEvaluation<Row> {
        @Override
        protected void evaluate(Row context, PValueTarget target) {
            PValueSource rowSource = null;
//            rowSource = context.rowEval(fieldIndex);
            target.putValueSource(rowSource);
        }

        @Override
        public void with(Row row) {
            setContext(row);
        }

        private Evaluation(PUnderlying underlyingType, int fieldIndex) {
            super(underlyingType);
            this.fieldIndex = fieldIndex;
        }

        private int fieldIndex;
    }
}
