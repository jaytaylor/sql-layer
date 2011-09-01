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
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.optimizer.ExpressionRow;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public abstract class ComposedExpressionTestBase {
    protected abstract int childrenCount();
    protected abstract Expression getExpression(List<? extends Expression> children);

    @Test
    public void childrenAreConst() {
        ExpressionEvaluation evaluation = evaluation(true, false, false);
        expectEvalSuccess(evaluation);
    }

    @Test
    public void childrenNeedRowAndBindings_HasNeither() {
        ExpressionEvaluation evaluation = evaluation(false, true, true);
        expectEvalError(evaluation);
    }

    @Test
    public void childrenNeedRowAndBindings_HasOnlyBindings() {
        ExpressionEvaluation evaluation = evaluation(false, true, true);
        evaluation.of(UndefBindings.only());
        expectEvalError(evaluation);
    }

    @Test
    public void childrenNeedRowAndBindings_HasBoth() {
        ExpressionEvaluation evaluation = evaluation(false, true, true);
        evaluation.of(dummyRow());
        expectEvalSuccess(evaluation);
    }

    @Test
    public void childrenNeedBindings_ButMissing() {
        ExpressionEvaluation evaluation = evaluation(false, false, true);
        expectEvalError(evaluation);
    }

    @Test
    public void childrenNeedBindings_AndHave() {
        ExpressionEvaluation evaluation = evaluation(false, false, true);
        evaluation.of(UndefBindings.only());
        expectEvalSuccess(evaluation);
    }

    @Test
    public void mutableNoArg() {
        ExpressionEvaluation evaluation = evaluation(false, false, false);
        expectEvalSuccess(evaluation);
    }

    private ExpressionEvaluation evaluation(boolean childConst, boolean childNeedsRow, boolean childNeedsBindings) {
        int childrenCount = childrenCount();
        if (childrenCount < 1)
            throw new UnsupportedOperationException("childrenCount() must be > 0");
        List<Expression> children = new ArrayList<Expression>();
        children.add(new DummyExpression(childConst, childNeedsRow, childNeedsBindings));
        for (int i=1; i < childrenCount; ++i) {
            children.add(CONST);
        }
        Expression expression = getExpression(children);
        assertEquals("isConstant", childConst, expression.isConstant());
        assertEquals("needsRow", childNeedsRow, expression.needsRow());
        assertEquals("needsBindings", childNeedsBindings, expression.needsBindings());
        return expression.evaluation();
    }

    private void expectEvalError(ExpressionEvaluation evaluation) {
        try {
            evaluation.eval().isNull();
            fail("expected an error");
        } catch (Exception e) {
            // ignore
        } catch (AssertionError e) {
            // ignore
        }
    }

    private void expectEvalSuccess(ExpressionEvaluation evaluation) {
        evaluation.eval().isNull();
    }

    private Row dummyRow() {
        return new ExpressionRow(null, null, null);
    }

    // consts

    private static final Expression CONST = new DummyExpression(true, false, false);

    // nested classes

    private static class DummyExpression implements Expression {

        @Override
        public boolean isConstant() {
            return requirements.isConstant;
        }

        @Override
        public boolean needsBindings() {
            return requirements.needsBindings;
        }

        @Override
        public boolean needsRow() {
            return requirements.needsRow;
        }

        @Override
        public ExpressionEvaluation evaluation() {
            return new DummyExpressionEvaluation(requirements);
        }

        @Override
        public AkType valueType() {
            return AkType.NULL;
        }

        private DummyExpression(boolean constant, boolean needsRow, boolean needsBindings) {
            requirements = new Requirements(constant, needsRow, needsBindings);
        }

        private final Requirements requirements;
    }

    private static class DummyExpressionEvaluation implements ExpressionEvaluation {
        @Override
        public void of(Row row) {
            hasRow = true;
        }

        @Override
        public void of(Bindings bindings) {
            hasBindings = true;
        }

        @Override
        public ValueSource eval() {
            if ( (requirements.needsBindings && !hasBindings) || (requirements.needsRow && ! hasRow)) {
                fail("failed requirements " + requirements + ": hasRow=" + hasRow + ", hasBindings=" + hasBindings);
            }
            return NullValueSource.only();
        }

        private DummyExpressionEvaluation(Requirements requirements) {
            this.requirements = requirements;
            hasRow = false;
            hasBindings = false;
        }

        private final Requirements requirements;
        private boolean hasRow;
        private boolean hasBindings;
    }

    private static class Requirements {

        private Requirements(boolean constant, boolean needsRow, boolean needsBindings) {
            isConstant = constant;
            this.needsRow = needsRow;
            this.needsBindings = needsBindings;
        }

        @Override
        public String toString() {
            List<String> reqs = new ArrayList<String>();
            if (isConstant)
                reqs.add("CONSTANT");
            if (needsRow)
                reqs.add("NEEDS ROW");
            if (needsBindings)
                reqs.add("NEEDS BINDINGS");
            return reqs.toString();
        }

        final boolean isConstant;
        final boolean needsRow;
        final boolean needsBindings;
    }
}
