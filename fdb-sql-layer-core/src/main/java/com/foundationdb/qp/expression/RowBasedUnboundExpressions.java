/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.expression;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TNullExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RowBasedUnboundExpressions implements UnboundExpressions {
    @Override
    public ValueRecord get(QueryContext context, QueryBindings bindings) {
        return new ExpressionsAndBindings(rowType, pExprs, context, bindings);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        for (TPreparedExpression expression : pExprs) {
            atts.put(Label.EXPRESSIONS, expression.getExplainer(context));
        }
        return new CompoundExplainer(Type.ROW, atts);
    }

    @Override
    public String toString() {
        return "UnboundExpressions" + pExprs;
    }

    // The extra, unused boolean parameter is to allow java to cleanly 
    // distinguish between this method and the next one. 
    public RowBasedUnboundExpressions(RowType rowType, List<ExpressionGenerator> expressions, boolean generate) {
        this(rowType, API.generateNew(expressions));
    }

    public RowBasedUnboundExpressions(RowType rowType, List<TPreparedExpression> pExprs)
    {
        assert pExprs != null : "must supply pExprs";
        for (TPreparedExpression expression : pExprs) {
            if (expression == null) {
                throw new IllegalArgumentException();
            }
        }
        this.pExprs = pExprs;
        this.rowType = rowType.schema().newProjectType(pExprs);
    }
    
    public boolean isLiteralNull(int index) {
        return pExprs.get(index).isLiteral();
    }

    private final List<TPreparedExpression> pExprs;
    private final RowType rowType;
    private static class ExpressionsAndBindings implements ValueRecord {

        @Override
        public ValueSource value(int index) {
            return expressionRow.value(index);
        }

        ExpressionsAndBindings(RowType rowType, List<TPreparedExpression> pExprs,
                               QueryContext context, QueryBindings bindings)
        {
            expressionRow = new ExpressionRow(rowType, context, bindings, pExprs);
        }
        
        @Override
        public String toString() {
            return expressionRow.toString();
        }

        private final ExpressionRow expressionRow;
        
        
    }
}
