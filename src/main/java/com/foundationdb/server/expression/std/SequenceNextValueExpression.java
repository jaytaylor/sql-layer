/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.expression.std;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class SequenceNextValueExpression extends AbstractBinaryExpression {

    @Scalar("NEXTVAL")
    public static final ExpressionComposer COMPOSER = new BinaryComposer() {
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType,
                                     ExpressionType secondType, ExpressionType resultType) {
            return new SequenceNextValueExpression(AkType.LONG, first, second);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.VARCHAR);
            return ExpressionTypes.LONG;
        }
    };

    public SequenceNextValueExpression(AkType type, Expression first, Expression second) {
        super(type, first, second);
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append("NEXTVAL(").append(left()).append(", ").append(right()).append(")");
    }

    @Override
    public boolean nullIsContaminating() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public String name() {
        return "NEXTVAL";
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation {

        private InnerEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
        }

        @Override
        public void of(QueryContext context) {
            super.of(context);
            needAnother = true;
        }

        @Override
        public void of(QueryBindings bindings) {
            super.of(bindings);
            needAnother = true;
        }

        @Override
        public void of(Row row) {
            super.of(row);
            needAnother = true;
        }

        @Override
        public ValueSource eval() {
            if (needAnother) {
                String schema = left().getString();
                String sequence = right().getString();
                logger.debug("Sequence loading : {}.{}", schema, sequence);

                TableName sequenceName = new TableName (schema, sequence);

                long value = queryContext().sequenceNextValue(sequenceName);
                valueHolder().putLong(value);
                needAnother = false;
            }
            return valueHolder();
        }

        private boolean needAnother;
    }

    private static final Logger logger = LoggerFactory.getLogger(SequenceNextValueExpression.class);
}
