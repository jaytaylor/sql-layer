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

import com.akiban.junit.OnlyIfNot;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.operator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.test.it.qp.NullsRow;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static com.akiban.server.expression.std.ComposedExpressionTestBase.ExpressionAttribute.*;
import static org.junit.Assert.*;

public abstract class ComposedExpressionTestBase {
    protected static class CompositionTestInfo
    {
        private final int childrenCount;
        private final AkType childrenType;
        private final boolean nullIsContaminating;

        public CompositionTestInfo (int argc, AkType type, boolean nullCont)
        {
            childrenCount = argc;
            childrenType = type;
            nullIsContaminating = nullCont;
        }

        public int getChildrenCount ()
        {
            return childrenCount;
        }

        public AkType getChildrenType ()
        {
            return childrenType;
        }

        public boolean nullIsContaminating ()
        {
            return nullIsContaminating;
        }
    }

    protected abstract CompositionTestInfo getTestInfo ();
    protected abstract ExpressionComposer getComposer();
    protected abstract boolean alreadyExc ();

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenAreConst() {
        ExpressionEvaluation evaluation = evaluation(IS_CONSTANT);
        expectEvalSuccess(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedRowAndBindings_HasNeither() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_BINDINGS, NEEDS_ROW);
        expectEvalError(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedRowAndBindings_HasOnlyBindings() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_BINDINGS, NEEDS_ROW);
        evaluation.of(UndefBindings.only());
        expectEvalError(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedRowAndBindings_HasOnlyRow() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_BINDINGS, NEEDS_ROW);
        evaluation.of(dummyRow());
        expectEvalError(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedRowAndBindings_HasBoth() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_BINDINGS, NEEDS_ROW);
        evaluation.of(dummyRow());
        evaluation.of(UndefBindings.only());
        expectEvalSuccess(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedBindings_ButMissing() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_BINDINGS);
        expectEvalError(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedBindings_AndHave() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_BINDINGS);
        evaluation.of(UndefBindings.only());
        expectEvalSuccess(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedRow_ButMissing() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_ROW);
        expectEvalError(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void childrenNeedRow_AndHave() {
        ExpressionEvaluation evaluation = evaluation(NEEDS_ROW);
        evaluation.of(dummyRow());
        expectEvalSuccess(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void mutableNoArg() {
        ExpressionEvaluation evaluation = evaluation();
        expectEvalSuccess(evaluation);
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void testSharing() {
        EvaluationPair pair = evaluationPair();
        ExpressionEvaluation evaluation = pair.evaluation;
        List<String> messages = pair.messages;

        assertEquals("isShared", false, evaluation.isShared());
        assertEquals("messages.size", 0, messages.size());

        // share once
        evaluation.acquire();
        checkMessages(messages, SHARE);
        assertEquals("isShared", true, evaluation.isShared());

        // share twice
        evaluation.acquire();
        checkMessages(messages, SHARE);
        assertEquals("isShared", true, evaluation.isShared());

        // release once
        evaluation.release();
        checkMessages(messages, RELEASE);
        assertEquals("isShared", true, evaluation.isShared());

        // release twice
        evaluation.release();
        checkMessages(messages, RELEASE);
        assertEquals("isShared", false, evaluation.isShared());
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void releasingWhenUnshared() {
        EvaluationPair pair = evaluationPair();
        ExpressionEvaluation evaluation = pair.evaluation;
        List<String> messages = pair.messages;
        evaluation.release();
        assertEquals("messages.size", 0, messages.size());
    }

    private void checkMessages(List<String> messages, String singleMessage) {
        List<String> expected = new ArrayList<String>();
        int children = getTestInfo().getChildrenCount();
        assert children > 0 : children;
        for (int i=0; i < children; ++i) {
            expected.add(singleMessage);
        }
        assertEquals(expected, messages);
        messages.clear();
    }

    private ExpressionEvaluation evaluation(ExpressionAttribute... attributes) {
        return evaluationPair(attributes).evaluation;
    }

    private EvaluationPair evaluationPair(ExpressionAttribute... attributes) {        
        int childrenCount = getTestInfo().getChildrenCount();
        if (childrenCount < 1)
            throw new UnsupportedOperationException("childrenCount() must be > 0");
        List<String> messages = new ArrayList<String>();
        List<Expression> children = new ArrayList<Expression>();
        children.add(new DummyExpression(messages, getTestInfo().getChildrenType(), attributes));
        for (int i=1; i < childrenCount; ++i) {
            children.add(new DummyExpression(messages, getTestInfo().getChildrenType(),IS_CONSTANT));
        }
        Expression expression = getComposer().compose(children);
        Set<ExpressionAttribute> attributeSet = attributesSet(attributes);
        assertEquals("isConstant", attributeSet.contains(IS_CONSTANT) || anyChildIsNull(children) && getTestInfo().nullIsContaminating() , expression.isConstant());
        assertEquals("needsRow", attributeSet.contains(NEEDS_ROW) && !(anyChildIsNull(children) && getTestInfo().nullIsContaminating()) , expression.needsRow());
        assertEquals("needsBindings", attributeSet.contains(NEEDS_BINDINGS) && !(anyChildIsNull(children) && getTestInfo().nullIsContaminating()), expression.needsBindings());
        return new EvaluationPair(expression.evaluation(), messages);
    }

    private boolean anyChildIsNull (List<Expression> children)
    {
        for (Expression child: children)
            if (child.valueType() == AkType.NULL) return true;
        return false;
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
        return new NullsRow(null);
    }
    
    private static Set<ExpressionAttribute> attributesSet(ExpressionAttribute[] attributes) {
        Set<ExpressionAttribute> result = EnumSet.noneOf(ExpressionAttribute.class);
        Collections.addAll(result, attributes);
        return result;
    }

    // consts

    private static final String SHARE = "share";
    private static final String RELEASE = "release";

    // nested classes

    private static class DummyExpression implements Expression {

        @Override
        public boolean isConstant() {
            return requirements.contains(IS_CONSTANT);
        }

        @Override
        public boolean needsBindings() {
            return requirements.contains(NEEDS_BINDINGS);
        }

        @Override
        public boolean needsRow() {
            return requirements.contains(NEEDS_ROW);
        }

        @Override
        public ExpressionEvaluation evaluation() {
            return new DummyExpressionEvaluation(messages, requirements);
        }

        @Override
        public AkType valueType() {
            return type;
        }

        @Override
        public String toString () {
            return getClass().getSimpleName() + " with " + requirements;
        }
        
        private DummyExpression(List<String> messages, AkType type, ExpressionAttribute... attributes) {
            requirements = attributesSet(attributes);
            this.messages = messages;
            this.type = type;
        }

        private final Set<ExpressionAttribute> requirements;
        private final List<String> messages;
        private final AkType type;
    }

    private static class DummyExpressionEvaluation implements ExpressionEvaluation {
        @Override
        public void of(Row row) {
            missingRequirements.remove(ExpressionAttribute.NEEDS_ROW);
        }

        @Override
        public void of(Bindings bindings) {
            missingRequirements.remove(ExpressionAttribute.NEEDS_BINDINGS);
        }

        @Override
        public void of(StoreAdapter adapter) {
        }

        @Override
        public ValueSource eval() {
            if (!missingRequirements.isEmpty()) {
                fail("failed requirements " + missingRequirements + ": original requirements were " + requirements);
            }
            return NullValueSource.only();
        }

        @Override
        public void acquire() {
            messages.add(SHARE);
            ++sharedBy;
        }

        @Override
        public boolean isShared() {
            return sharedBy > 0;
        }

        @Override
        public void release() {
            if (sharedBy > 0) {
                messages.add(RELEASE);
                --sharedBy;
            }
        }

        private DummyExpressionEvaluation(List<String> messages, Set<ExpressionAttribute> requirements) {
            this.requirements = requirements;
            this.missingRequirements = EnumSet.copyOf(requirements);
            missingRequirements.remove(IS_CONSTANT);
            this.messages = messages;
        }

        private final Set<ExpressionAttribute> requirements;
        private final Set<ExpressionAttribute> missingRequirements;
        private final List<String> messages;
        private int sharedBy = 0;
    }

    enum ExpressionAttribute {
        IS_CONSTANT,
        NEEDS_ROW,
        NEEDS_BINDINGS
    }

    private static class EvaluationPair {
        private EvaluationPair(ExpressionEvaluation evaluation, List<String> messages) {
            this.evaluation = evaluation;
            this.messages = messages;
        }

        public final ExpressionEvaluation evaluation;
        public final List<String> messages;
    }
}
