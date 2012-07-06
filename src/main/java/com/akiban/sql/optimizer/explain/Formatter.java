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

package com.akiban.sql.optimizer.explain;

import com.akiban.sql.optimizer.explain.Type.GeneralType;
import java.util.Map;

public class Formatter {

    String describe(Explainer explainer) {
        StringBuilder sb = new StringBuilder("");
        describe(explainer, sb);
        return sb.toString();
    }
    
    void describe(Explainer explainer, StringBuilder sb) {
        describe(explainer, sb, false, null);
    }

    void describe(Explainer explainer, StringBuilder sb, boolean needsParens, String parentName) {
        if (explainer.hasAttributes())
        {
            OperationExplainer opEx = (OperationExplainer) explainer;
            if (explainer.getType().generalType() == GeneralType.OPERATOR)
                describeOperator(opEx, sb);
            else
                describeExpression(opEx, sb, needsParens, parentName);
        }
        else
        {
            PrimitiveExplainer primEx = (PrimitiveExplainer) explainer;
            describePrimitive(primEx, sb);
        }
    }

    void describeExpression(OperationExplainer explainer, StringBuilder sb, boolean needsParens, String parentName) {
        
        Attributes atts = (Attributes) explainer.get().clone();
        
        if (explainer.get().containsKey(Label.INFIX))
        {
            Explainer leftExplainer = atts.valuePairs().get(0).getValue();
            Explainer rightExplainer = atts.valuePairs().get(1).getValue();
            String name = atts.get(Label.NAME).get(0).get().toString();
            if (name.equals(parentName) && explainer.get().containsKey(Label.ASSOCIATIVE))
                needsParens = false;
            if (needsParens)
                sb.append("(");
            describe(leftExplainer, sb, true, name);
            sb.append(" ").append(name).append(" ");
            describe(rightExplainer, sb, true, name);
            if (needsParens)
                sb.append(")");
        }
        else
        {
            sb.append(atts.get(Label.NAME).get(0).get());
            atts.remove(Label.NAME);
            sb.append("(");
            for (Map.Entry<Label, Explainer> entry : atts.valuePairs())
            {
                describe(entry.getValue(), sb);
                sb.append(", ");
            }
            sb.setLength(sb.length()-2);
            sb.append(")");
        }
    }

    void describePrimitive(PrimitiveExplainer explainer, StringBuilder sb) {
        if (explainer.getType()==Type.STRING)
        {
            sb.append("\"").append(explainer.get()).append("\"");
        }
        else
        {
            sb.append(explainer.get());
        }
    }

    void describeOperator(OperationExplainer opEx, StringBuilder sb) {
        Type type = opEx.getType();
        switch (type) 
        {
            case SELECT_HKEY:
                break;
            case PROJECT:
                break;
            default:
                throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
