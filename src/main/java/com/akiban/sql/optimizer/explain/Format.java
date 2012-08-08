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
import java.util.ArrayList;
import java.util.List;

public class Format {
    
    private boolean verbose = true;
    private int numSubqueries = 0;
    private List<Explainer> subqueries = new ArrayList<Explainer>();
    private StringBuilder sb = new StringBuilder("");
    private List<String> rows = new ArrayList<String>();
    
    public Format(boolean verbose)
    {
        this.verbose = verbose;
    }

    public List<String> Describe(Explainer explainer)
    {
        describe(explainer);
        for (int i = 1; i <= numSubqueries; i++)
        {
            newRow();
            sb.append("SUBQUERY ").append(i).append(':');
            describe(subqueries.get(i-1));
        }
        newRow();
        return rows;
    }
    
    private void describe(Explainer explainer) {
        describe(explainer, sb, false, null);
    }

    protected void describe(Explainer explainer, StringBuilder sb, boolean needsParens, String parentName) {
        if (explainer.hasAttributes())
        {
            OperationExplainer opEx = (OperationExplainer) explainer;
            if (explainer.getType().generalType() == GeneralType.OPERATOR)
                describeOperator(opEx, 0);
            else
                describeExpression(opEx, needsParens, parentName);
        }
        else
        {
            PrimitiveExplainer primEx = (PrimitiveExplainer) explainer;
            describePrimitive(primEx);
        }
    }

    protected void describeExpression(OperationExplainer explainer, boolean needsParens, String parentName) {
        
        Attributes atts = explainer.get();
        String name = atts.get(Label.NAME).get(0).get().toString();
        
        if (atts.containsKey(Label.INFIX_REPRESENTATION))
        {
            Explainer leftExplainer = atts.valuePairs().get(0).getValue();
            Explainer rightExplainer = atts.valuePairs().get(1).getValue();
            if (name.equals(parentName) && atts.containsKey(Label.ASSOCIATIVE))
            {
                if(atts.get(Label.ASSOCIATIVE).get(0).equals(PrimitiveExplainer.getInstance(true)))
                    needsParens = false;
            }
            if (needsParens)
                sb.append("(");
            describe(leftExplainer, sb, true, name);
            sb.append(" ").append(atts.get(Label.INFIX_REPRESENTATION).get(0).get()).append(" ");
            describe(rightExplainer, sb, true, name);
            if (needsParens)
                sb.append(")");
        }
        else if (explainer.getType().equals(Type.LITERAL))
        {
            sb.append(atts.get(Label.OPERAND).get(0).get());
        }
        else if (explainer.getType().equals(Type.SUBQUERY))
        {
            sb.append("SUBQUERY ").append(++numSubqueries);
            subqueries.add(atts.get(Label.OPERAND).get(0));
        }
        else if (name.startsWith("CAST"))
        {
            sb.append(name.substring(0, 4)).append("(");
            describe(atts.get(Label.OPERAND).get(0));
            sb.append(" AS ").append(atts.get(Label.OUTPUT_TYPE).get(0).get()).append(")");
        }
        else if (name.equals("Bound"))
        {
            sb.append(name).append("(").append(atts.get(Label.BINDING_POSITION).get(0).get()).append(",");
            describe(atts.get(Label.OPERAND).get(0));
            sb.append(")");
        }
        else if (name.equals("Variable"))
        {
            sb.append(name).append("(pos=").append(atts.get(Label.BINDING_POSITION).get(0).get()).append(")");
        }
        else
        {
            sb.append(name).append("(");
            if (atts.containsKey(Label.OPERAND))
            {
                for (Explainer entry : atts.get(Label.OPERAND))
                {
                    describe(entry);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
            }
            sb.append(")");
        }
    }

    protected void describePrimitive(PrimitiveExplainer explainer) {

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

    protected void describeOperator(OperationExplainer explainer, int depth) {
        
        Attributes atts = explainer.get();
        String name = atts.get(Label.NAME).get(0).get().toString();
        Type type = explainer.getType();
        
        if (!verbose)
        {
            sb.append(name.substring(0, name.indexOf('_')));
            switch (type)
            {
                case LOOKUP_OPERATOR:
                    sb.append('(');
                    for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                    {
                        describe(table);
                        sb.append(", ");
                    }
                    if (!atts.get(Label.ANCESTOR_TYPE).isEmpty())
                    {
                        sb.setLength(sb.length() - 2);
                    }
                    sb.append(')');
                    break;
                case COUNT_OPERATOR:
                    sb.append("(*)");
                    break;
                case PHYSICAL_OPERATOR:
                    sb.append('(').append(atts.get(Label.BRIEF)).append(')');
                    break;
                default:
                    // Nothing needed, as there are many operators which display nothing in brief mode
            }
        }
        else
        {
            sb.append(name).append("(");
            switch (type) 
            {
                case SELECT_HKEY:
                    describe(atts.get(Label.PREDICATE).get(0));
                    break;
                case PROJECT:
                    for (Explainer projection : atts.get(Label.PROJECTION))
                    {
                        describe(projection);
                        sb.append(", ");
                    }
                    sb.setLength(sb.length()-2);
                    break;
                case SCAN_OPERATOR:
                    if (name.equals("Values Scan"))
                    {
                        for (Explainer row : atts.get(Label.ROWTYPE))
                        {
                            describe(row);
                            sb.append(", ");
                        }
                        if (!atts.valuePairs().isEmpty())
                        {
                            sb.setLength(sb.length() - 2);
                        }
                    }
                    else if (name.equals("Group Scan"))
                    {
                        describe(atts.get(Label.GROUP_TABLE).get(0));
                    }
                    //else if (name.equals("Index Scan"))
                    {
                        // TODO
                    }
                    break;
                case LOOKUP_OPERATOR:
                    if (name.equals("Ancestor Lookup Default"))
                    {
                        describe(atts.get(Label.GROUP_TABLE).get(0));
                        sb.append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                        {
                            for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                                sb.append(table.get()).append(", ");
                            if (!atts.get(Label.ANCESTOR_TYPE).isEmpty())
                            {
                                sb.setLength(sb.length() - 2);
                            }
                        }
                    }
                    else if (name.equals("Ancestor Lookup Nested"))
                    {
                        sb.append(atts.get(Label.BINDING_POSITION).get(0).get()).append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                        {
                            for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                                sb.append(table.get()).append(", ");
                            if (!atts.get(Label.ANCESTOR_TYPE).isEmpty())
                            {
                                sb.setLength(sb.length() - 2);
                            }
                        }
                    }
                    else if (name.equals("Branch Lookup Default"))
                    {
                        describe(atts.get(Label.GROUP_TABLE).get(0));
                        sb.append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                            describe(atts.get(Label.OUTPUT_TYPE).get(0));
                        sb.append(" (via ");
                        describe(atts.get(Label.ANCESTOR_TYPE).get(0));
                        sb.append(")");
                    }
                    else if (name.equals("Branch Lookup Nested"))
                    {
                        sb.append(atts.get(Label.BINDING_POSITION).get(0).get()).append(" -> ");
                        if (atts.containsKey(Label.TABLE_CORRELATION))
                        {
                            for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                                sb.append(table.get()).append(", ");
                            sb.setLength(sb.length()-2);
                        }
                        else
                            describe(atts.get(Label.OUTPUT_TYPE).get(0));
                        sb.append(" (via ");
                        describe(atts.get(Label.ANCESTOR_TYPE).get(0));
                        sb.append(")");
                    }
                    break;
                case COUNT_OPERATOR:
                    sb.append("*");
                    if (name.equals("Count TableStatus"));
                    {
                        sb.append(" FROM ");
                        describe(atts.get(Label.INPUT_TYPE).get(0));
                    }
                    break;
                case DISTINCT:
                case UNION_ALL:
                    break;
                case FILTER:
                    for (Explainer rowtype : atts.get(Label.KEEP_TYPE))
                    {
                        describe(rowtype);
                        sb.append(" - ");
                    }
                    if (!atts.get(Label.KEEP_TYPE).isEmpty())
                    {
                        sb.setLength(sb.length()-3);
                    }
                    break;
                case FLATTEN_OPERATOR:
                    sb.append(atts.get(Label.PARENT_TYPE).get(0).get()).append(" ").append
                            (atts.get(Label.JOIN_OPTION).get(0).get()).append(" ").append(atts.get(Label.CHILD_TYPE).get(0).get());
                    break;
                case ORDERED:
                    sb.append("skip ");
                    describe(atts.get(Label.LEFT).get(0));
                    sb.append(" left, skip ");
                    describe(atts.get(Label.RIGHT).get(0));
                    sb.append(" right, compare ");
                    describe(atts.get(Label.NUM_COMPARE).get(0));
                    if (name.equals("HKeyUnion"))
                    {
                        sb.append(", shorten to ");
                        describe(atts.get(Label.OUTPUT_TYPE).get(0));
                    }
                    else if (name.equals("Intersect"))
                    {
                        sb.append(", USING ");
                        describe(atts.get(Label.JOIN_OPTION).get(0));
                    }
                    break;
                case IF_EMPTY:
                    for (Explainer expression : atts.get(Label.OPERAND))
                    {
                        describe(expression);
                        sb.append(", ");
                    }
                    if (!atts.valuePairs().isEmpty())
                    {
                        sb.setLength(sb.length() - 2);
                    }
                    break;
                case LIMIT_OPERATOR:
                    describe(atts.get(Label.LIMIT).get(0));
                    break;
                case NESTED_LOOPS:
                    if (name.equals("Map_NestedLoops"))
                    {
                        if(atts.containsKey(Label.TABLE_CORRELATION))
                            sb.append(atts.get(Label.TABLE_CORRELATION).get(0));
                        else
                            sb.append("loop_").append(atts.get(Label.BINDING_POSITION).get(0));
                    }
                    else if (name.equals("Product"))
                    {
                        describe(atts.get(Label.INNER_TYPE).get(0));
                        sb.append(" x ");
                        describe(atts.get(Label.OUTER_TYPE).get(0));
                    }
                    break;
                case SORT:
                    int i = 0;
                    for (Explainer ex : atts.get(Label.EXPRESSIONS))
                    {
                        describe(ex);
                        sb.append(" ").append(atts.get(Label.ORDERING).get(i++).get()).append(", ");
                    }
                    if (atts.containsKey(Label.LIMIT))
                    {
                        sb.append("LIMIT ");
                        describe(atts.get(Label.LIMIT).get(0));
                    }
                    sb.append(atts.get(Label.SORT_OPTION).get(0).get());
                    break;
                case DUI:
                    if (name.equals("Delete"))
                    {
                        sb.append(" FROM ");
                        describe(atts.get(Label.TABLE_TYPE).get(0));
                    }
                    else if (name.equals("Insert"))
                    {
                        sb.append("INTO");
                        describe(atts.get(Label.TABLE_TYPE).get(0));
                    }
                    // TODO: "Update"
                    break;
                case PHYSICAL_OPERATOR:
                    sb.append(atts.get(Label.GROUPING_OPTION).get(0)).append(": ");
                    for (Explainer ex : atts.get(Label.AGGREGATORS))
                    {
                        sb.append(ex.get());
                        sb.append(", ");
                    }
                    sb.setLength(sb.length()-2);
                    break;
                default:
                    throw new UnsupportedOperationException("Formatter does not recognize " + type.name());
            }
            sb.append(")");
        }
        if (atts.containsKey(Label.INPUT_OPERATOR))
        {
            for (Explainer input : atts.get(Label.INPUT_OPERATOR))
            {
                newRow();
                for (int i = 0; i <= depth; i++)
                {
                    sb.append("  ");
                }
                describeOperator((OperationExplainer) input, depth + 1);
            }
        }
        if (atts.containsKey(Label.INNER_OPERATOR))
        {
            for (Explainer input : atts.get(Label.INNER_OPERATOR))
            {
                newRow();
                for (int i = 0; i <= depth; i++)
                {
                    sb.append("  ");
                }
                describeOperator((OperationExplainer) input, depth + 1);
            }
        }
        if (atts.containsKey(Label.OUTER_OPERATOR))
        {
            for (Explainer input : atts.get(Label.OUTER_OPERATOR))
            {
                newRow();
                for (int i = 0; i <= depth; i++)
                {
                    sb.append("  ");
                }
                describeOperator((OperationExplainer) input, depth + 1);
            }
        }
    }
    
    private void newRow()
    {
        rows.add(sb.toString());
        sb.delete(0, sb.length());
    }
}
