/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionComposer.NullTreating;
import com.akiban.junit.OnlyIfNot;
import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.Explainer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionType;
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
import java.util.*;
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

    protected static Expression compose(ExpressionComposer composer, List<? extends Expression> arguments) {
        return composer.compose(arguments, Collections.<ExpressionType>nCopies(arguments.size() + 1, null));
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void isNullSpecial() // make sure two two methods in ExpressionComposer and Expression match
    {        
        List<Expression> children = new ArrayList<Expression>();
        List<String> messages = new ArrayList<String>();
        children.add(new DummyExpression(messages, getTestInfo().getChildrenType(), new ExpressionAttribute[]{IS_CONSTANT}));
        for (int i = 1; i < getTestInfo().getChildrenCount(); ++i)
            children.add(new DummyExpression(messages, getTestInfo().getChildrenType(), IS_CONSTANT));
        
        Expression expression = compose(getComposer(), children);
        assertTrue ("ExpressionComposer.nullIsContaminating() and Expression.nullIsContaminating() should match",
                    (getComposer().getNullTreating() == NullTreating.RETURN_NULL) == expression.nullIsContaminating());
    }
    
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
        evaluation.of(dummyBindings());
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
        evaluation.of(dummyBindings());
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
        evaluation.of(dummyBindings());
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
        Expression expression = compose(getComposer(), children);
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
    
    private QueryContext dummyBindings() {
        return new SimpleQueryContext(null);
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

        @Override
        public String name()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Explainer getExplainer(Map<Object, Explainer> extraInfo)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
        public boolean nullIsContaminating()
        {
            return true;
        }
    }

    private static class DummyExpressionEvaluation extends ExpressionEvaluation.Base {
        @Override
        public void of(Row row) {
            missingRequirements.remove(ExpressionAttribute.NEEDS_ROW);
        }

        @Override
        public void of(QueryContext context) {
            missingRequirements.remove(ExpressionAttribute.NEEDS_BINDINGS);
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
