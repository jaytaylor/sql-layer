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

import com.akiban.server.types.AkType;
import com.akiban.server.types.util.SqlLiteralValueFormatter;
import com.akiban.sql.optimizer.explain.Type.GeneralType;
import java.util.Map;

public class Format {

    public static String Describe(Explainer explainer) {
        StringBuilder sb = new StringBuilder("");
        describe(explainer, sb);
        return sb.toString();
    }
    
    private static void describe(Explainer explainer, StringBuilder sb) {
        describe(explainer, sb, false, null);
    }

    protected static void describe(Explainer explainer, StringBuilder sb, boolean needsParens, String parentName) {
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

    protected static void describeExpression(OperationExplainer explainer, StringBuilder sb, boolean needsParens, String parentName) {
        
        Attributes atts = (Attributes) explainer.get().clone();
        String name = atts.get(Label.NAME).get(0).get().toString();
        
        if (explainer.getType().equals(Type.LITERAL))
        {
            sb.append(atts.get(Label.OPERAND).get(0).get());
        }
        else if (name.equals("Field"))
        {
            sb.append(name).append("(").append(atts.get(Label.BINDING_POSITION).get(0).get()).append(")");
        }
        else if (name.equals("Bound"))
        {
            sb.append(name).append("(").append(atts.get(Label.BINDING_POSITION).get(0).get()).append(",");
            describe(atts.get(Label.OPERAND).get(0), sb);
            sb.append(")");
        }
        else if (name.equals("Variable"))
        {
            sb.append(name).append("(pos=").append(atts.get(Label.BINDING_POSITION).get(0).get()).append(")");
        }
        else if (atts.containsKey(Label.INFIX_REPRESENTATION))
        {
            Explainer leftExplainer = atts.valuePairs().get(0).getValue();
            Explainer rightExplainer = atts.valuePairs().get(1).getValue();
            if (name.equals(parentName) && atts.get(Label.ASSOCIATIVE).get(0).equals(PrimitiveExplainer.getInstance(true)))
                needsParens = false;
            if (needsParens)
                sb.append("(");
            describe(leftExplainer, sb, true, name);
            sb.append(" ").append(atts.get(Label.INFIX_REPRESENTATION).get(0).get()).append(" ");
            describe(rightExplainer, sb, true, name);
            if (needsParens)
                sb.append(")");
        }
        else
        {
            sb.append(name).append("(");
            if (atts.containsKey(Label.OPERAND))
            {
                for (Explainer entry : atts.get(Label.OPERAND))
                {
                    describe(entry, sb);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
            }
            sb.append(")");
        }
    }

    protected static void describePrimitive(PrimitiveExplainer explainer, StringBuilder sb) {

        if (explainer.getType()==Type.STRING)
        {
            sb.append('\'').append(explainer.get()).append('\'');
        }
        /*else if (explainer.getClass().isArray())
        {
        * do we need functionality to describePrimitive each element of the array separately?
        }*/
        else
        {
            sb.append(explainer.get());
        }
        
    }

    protected static void describeOperator(OperationExplainer explainer, StringBuilder sb) {
        
        Attributes atts = (Attributes) explainer.get().clone();
        String name = atts.get(Label.NAME).get(0).get().toString();
        
        sb.append(name).append("(");
        
        Type type = explainer.getType();
        switch (type) 
        {
            case SELECT_HKEY:
                describe(atts.get(Label.PREDICATE).get(0),sb);
                break;
            case PROJECT:
                for (Explainer projection : atts.get(Label.PROJECTION))
                {
                    describe(projection, sb);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
                break;
            case SCAN_OPERATOR:
                if (name.equals("Values Scan"))
                {
                    for (Explainer row : atts.get(Label.ROWTYPE))
                    {
                        describe(row, sb);
                        sb.append(", ");
                    }
                    if (!atts.valuePairs().isEmpty())
                    {
                        sb.setLength(sb.length() - 2);
                    }
                }
                else if (name.equals("Group Scan"))
                {
                    describe(atts.get(Label.GROUP_TABLE).get(0), sb);
                }
                //else if (name.equals("Index Scan"))
                {
                    // TODO
                }
                break;
            case LOOKUP_OPERATOR:
                if (name.equals("Ancestor Lookup Default"))
                {
                    describe(atts.get(Label.INPUT_OPERATOR).get(0), sb);
                    sb.append(" -> ");
                    for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                    {
                        describe(table, sb);
                        sb.append(", ");
                    }
                    if (!atts.valuePairs().isEmpty())
                    {
                        sb.setLength(sb.length() - 2);
                    }
                }
                else if (name.equals("Ancestor Lookup Nested"))
                {
                    describe(atts.get(Label.BINDING_POSITION).get(0), sb);
                    sb.append(" -> ");
                    for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                    {
                        describe(table, sb);
                        sb.append(", ");
                    }
                    if (!atts.valuePairs().isEmpty())
                    {
                        sb.setLength(sb.length() - 2);
                    }
                }
                else if (name.equals("Branch Lookup Default"))
                {
                    describe(atts.get(Label.INPUT_OPERATOR).get(0), sb);
                    sb.append(" -> ");
                    describe(atts.get(Label.OUTPUT_TYPE).get(0), sb);
                    sb.append(" (via ");
                    describe(atts.get(Label.ANCESTOR_TYPE).get(0), sb);
                    sb.append(")");
                }
                else if (name.equals("Branch Lookup Nested"))
                {
                    describe(atts.get(Label.BINDING_POSITION).get(0), sb);
                    sb.append(" -> ");
                    describe(atts.get(Label.OUTPUT_TYPE).get(0), sb);
                    sb.append(" (via ");
                    describe(atts.get(Label.ANCESTOR_TYPE).get(0), sb);
                    sb.append(")");
                }
                break;
            case COUNT_OPERATOR:
                if (name.equals("Count Default"))
                {
                    sb.append("*");
                }
                else if (name.equals("Count TableStatus"));
                {
                    sb.append("* FROM ");
                    describe(atts.get(Label.INPUT_TYPE).get(0), sb);
                }
                break;
            case DISTINCT:
                describe(atts.get(Label.INPUT_OPERATOR).get(0), sb);
            case FILTER:
                for (Explainer rowtype : atts.get(Label.KEEP_TYPE))
                {
                    describe(rowtype, sb);
                    sb.append(" - ");
                }
                sb.setLength(sb.length()-3);
                break;
            case FLATTEN_OPERATOR: // Eventually may want to implement associativity for this
                describe(atts.get(Label.PARENT_TYPE).get(0), sb);
                sb.append(" ");
                describe(atts.get(Label.JOIN_OPTION).get(0), sb);
                sb.append(" ");
                describe(atts.get(Label.CHILD_TYPE).get(0), sb);
                break;
            case ORDERED:
                sb.append("skip ");
                describe(atts.get(Label.LEFT).get(0), sb);
                sb.append(" left, skip ");
                describe(atts.get(Label.RIGHT).get(0), sb);
                sb.append(" right, compare ");
                describe(atts.get(Label.NUM_COMPARE).get(0), sb);
                if (name.equals("HKeyUnion"))
                {
                    sb.append(", shorten to ");
                    describe(atts.get(Label.OUTPUT_TYPE).get(0), sb);
                }
                else if (name.equals("Intersect"))
                {
                    sb.append(", USING ");
                    describe(atts.get(Label.JOIN_OPTION).get(0), sb);
                }
                break;
            case IF_EMPTY:
                for (Explainer expression : atts.get(Label.OPERAND))
                {
                    describe(expression, sb);
                    sb.append(", ");
                }
                if (!atts.valuePairs().isEmpty())
                {
                    sb.setLength(sb.length() - 2);
                }
                break;
            case LIMIT_OPERATOR:
                describe(atts.get(Label.LIMIT).get(0), sb);
                break;
            case NESTED_LOOPS:
                if (name.equals("Map"))
                {
                    sb.append("Binding at ");
                    describe(atts.get(Label.BINDING_POSITION).get(0), sb);
                }
                else if (name.equals("Product"))
                {
                    describe(atts.get(Label.INNER_TYPE).get(0), sb);
                    sb.append(" x ");
                    describe(atts.get(Label.OUTER_TYPE).get(0), sb);
                }
                break;
            case SORT:
                describe(atts.get(Label.INPUT_OPERATOR).get(0), sb);
                sb.append(", ").append(atts.get(Label.SORT_OPTION).get(0).get());
                break;
            default:
                throw new UnsupportedOperationException("Formatter does not recognize " + type.name());
        }
        sb.append(")");
    }
}
