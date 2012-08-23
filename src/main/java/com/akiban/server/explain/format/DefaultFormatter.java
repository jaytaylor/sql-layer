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

package com.akiban.server.explain.format;

import com.akiban.server.explain.*;
import com.akiban.server.explain.Type.GeneralType;

import java.util.ArrayList;
import java.util.List;

public class DefaultFormatter
{
    private String defaultSchemaName;
    private boolean verbose;
    private int numSubqueries = 0;
    private List<CompoundExplainer> subqueries = new ArrayList<CompoundExplainer>();
    private StringBuilder sb = new StringBuilder();
    private List<String> rows = new ArrayList<String>();
    
    public DefaultFormatter(String defaultSchemaName, boolean verbose) {
        this.defaultSchemaName = defaultSchemaName;
        this.verbose = verbose;
    }

    public List<String> format(Explainer explainer) {
        append(explainer);
        for (int i = 1; i <= numSubqueries; i++) {
            newRow();
            sb.append("SUBQUERY ").append(i).append(':');
            newRow();
            appendOperator(subqueries.get(i-1), 0);
        }
        newRow();
        return rows;
    }
    
    protected void append(Explainer explainer) {
        append(explainer, false, null);
    }

    protected void append(Explainer explainer, boolean needsParens, String parentName) {
        switch (explainer.getType().generalType()) {
        case SCALAR_VALUE:
            appendPrimitive((PrimitiveExplainer)explainer);
            break;
        case OPERATOR:
            appendOperator((CompoundExplainer)explainer, 0);
            break;
        case ROWTYPE:
            appendRowType((CompoundExplainer)explainer);
            break;
        default:
            appendExpression((CompoundExplainer)explainer, needsParens, parentName);
        }
    }

    protected void appendPrimitive(PrimitiveExplainer explainer) {
        sb.append(explainer.get());
    }

    protected void appendExpression(CompoundExplainer explainer, boolean needsParens, String parentName) {
        switch (explainer.getType()) {
        case FIELD:
            appendField(explainer);
            break;
        case FUNCTION:
        case BINARY_OPERATOR:
        case TYPES3:
            appendFunction(explainer, needsParens, parentName);
            break;
        case SUBQUERY:
            appendSubquery(explainer);
            break;
        case LITERAL:
            appendLiteral(explainer);
            break;
        case VARIABLE:
            appendVariable(explainer);
            break;
        }
    }

    protected void appendFunction(CompoundExplainer explainer, boolean needsParens, String parentName) {
        Attributes atts = explainer.get();
        String name = atts.getValue(Label.NAME).toString();
        
        if (atts.containsKey(Label.INFIX_REPRESENTATION)) {
            Explainer leftExplainer = atts.valuePairs().get(0).getValue();
            Explainer rightExplainer = atts.valuePairs().get(1).getValue();
            if (name.equals(parentName) && atts.containsKey(Label.ASSOCIATIVE)) {
                if (Boolean.TRUE.equals(atts.getValue(Label.ASSOCIATIVE)))
                    needsParens = false;
            }
            if (needsParens)
                sb.append("(");
            append(leftExplainer, true, name);
            sb.append(" ").append(atts.getValue(Label.INFIX_REPRESENTATION)).append(" ");
            append(rightExplainer, true, name);
            if (needsParens)
                sb.append(")");
        }
        else if (name.startsWith("CAST")) {
            sb.append(name.substring(0, 4)).append("(");
            append(atts.getAttribute(Label.OPERAND));
            sb.append(" AS ").append(atts.getValue(Label.OUTPUT_TYPE)).append(")");
        }
        else {
            sb.append(name).append('(');
            if (atts.containsKey(Label.OPERAND)) {
                for (Explainer entry : atts.get(Label.OPERAND)) {
                    append(entry);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
            }
            sb.append(')');
        }
    }

    protected void appendField(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        boolean started = false;
        if (atts.containsKey(Label.TABLE_CORRELATION)) {
            sb.append(atts.getValue(Label.TABLE_CORRELATION));
            started = true;
        }
        else if (atts.containsKey(Label.TABLE_NAME)) {
            appendTableName(atts);
            started = true;
        }
        if (atts.containsKey(Label.COLUMN_NAME)) {
            if (started) sb.append(".");
            sb.append(atts.getValue(Label.COLUMN_NAME));
        }
        else if (started) {
            sb.append('[').append(atts.getValue(Label.POSITION)).append(']');
        }
        else {
            sb.append(atts.getValue(Label.NAME)).append('(');
            if (atts.containsKey(Label.BINDING_POSITION)) {
                sb.append(atts.getValue(Label.BINDING_POSITION)).append(", ");
            }
            sb.append(atts.getValue(Label.POSITION)).append(')');
        }
    }

    protected void appendSubquery(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        sb.append("SUBQUERY ").append(++numSubqueries);
        subqueries.add((CompoundExplainer)atts.getAttribute(Label.OPERAND));
    }

    protected void appendLiteral(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        sb.append(atts.getValue(Label.OPERAND));
    }

    protected void appendVariable(CompoundExplainer explainer) {
        Attributes atts = explainer.get();
        int pos = ((Number)atts.getValue(Label.BINDING_POSITION)).intValue();
        sb.append("$").append(pos+1);
    }

    protected void appendOperator(CompoundExplainer explainer, int depth) {
        
        Attributes atts = explainer.get();
        Type type = explainer.getType();
        String name = atts.getValue(Label.NAME).toString();
        
        if (!verbose) {
            sb.append(name.substring(0, name.indexOf('_')));
            switch (type) {
            case LOOKUP_OPERATOR:
                sb.append('(');
                if (atts.containsKey(Label.TABLE_CORRELATION)) {
                    for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                        sb.append(table.get()).append(", ");
                    sb.setLength(sb.length()-2);
                }
                else {
                    for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                        sb.append(table.get()).append(", ");
                    if (!atts.get(Label.ANCESTOR_TYPE).isEmpty())
                        sb.setLength(sb.length() - 2);
                }
                sb.append(')');
                break;
            case COUNT_OPERATOR:
                sb.append("(*)");
                break;
            case AGGREGATE:
                sb.append('(').append(atts.get(Label.BRIEF)).append(')');
                break;
            case DUI:
                if (name.equals("Delete_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_TYPE));
                }
                else if (name.equals("Insert_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_TYPE));
                }
                break;
            case SCAN_OPERATOR:
                if (name.equals("IndexScan_Default"))
                    sb.append(atts.getValue(Label.INDEX));
                break;
            default:
                // Nothing needed, as most operators display nothing in brief mode
            }
        }
        else {
            sb.append(name).append("(");
            switch (type) {
            case SELECT_HKEY:
                append(atts.getAttribute(Label.PREDICATE));
                break;
            case PROJECT:
                for (Explainer projection : atts.get(Label.PROJECTION)) {
                    append(projection);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
                break;
            case SCAN_OPERATOR:
                if (name.equals("ValuesScan_Default")) {
                    if (atts.containsKey(Label.EXPRESSIONS)) {
                        for (Explainer row : atts.get(Label.EXPRESSIONS))
                            sb.append(row.get()).append(", ");
                        sb.setLength(sb.length() - 2);
                    }
                }
                else if (name.equals("GroupScan_Default")) {
                    String opt = (String)atts.getValue(Label.SCAN_OPTION);
                    if (!opt.equals("full scan"))
                        sb.append(opt).append(" on ");
                    appendTableName(atts);
                }
                else if (name.equals("IndexScan_Default")) {
                    sb.append(atts.getValue(Label.INDEX));
                    if (atts.containsKey(Label.COLUMN_NAME)) {
                        int i = 0;
                        if (atts.containsKey(Label.EQUAL_COMPARAND))
                            for (Explainer comparand : atts.get(Label.EQUAL_COMPARAND))
                                sb.append(", ").append(atts.get(Label.COLUMN_NAME).get(i++).get()).append(" = ").append(comparand.get());
                        for (boolean first = true; i < atts.get(Label.COLUMN_NAME).size(); i++) {
                            sb.append(", ").append(atts.get(Label.COLUMN_NAME).get(i).get());
                            if (first) {
                                Object hi, lo;
                                if (atts.containsKey(Label.HIGH_COMPARAND)) {
                                    hi = atts.get(Label.HIGH_COMPARAND).get(0).get();
                                    if (atts.containsKey(Label.LOW_COMPARAND)) {
                                        lo = atts.get(Label.LOW_COMPARAND).get(0).get();
                                        if (atts.get(Label.HIGH_COMPARAND).get(1).get().equals("INCLUSIVE"))
                                            if (atts.get(Label.LOW_COMPARAND).get(1).get().equals("INCLUSIVE"))
                                                sb.append(" BETWEEN ").append(lo).append(" AND ").append(hi);
                                            else
                                                sb.append(" <= ").append(hi).append(" AND ").append(" > ").append(lo);
                                        else
                                            sb.append(" < ").append(hi).append(" AND ").append(atts.get(Label.LOW_COMPARAND).get(1).get().equals("INCLUSIVE") ? " >= " : " > ").append(lo);
                                    }
                                    else
                                        sb.append(atts.get(Label.HIGH_COMPARAND).get(1).get().equals("INCLUSIVE") ? " <= " : " < ").append(hi);
                                }
                                else if (atts.containsKey(Label.LOW_COMPARAND))
                                    sb.append(atts.get(Label.LOW_COMPARAND).get(1).get().equals("INCLUSIVE") ? " >= " : " > ").append(atts.get(Label.LOW_COMPARAND).get(0).get());
                                first = false;
                            }
                        }
                    }
                }
                break;
            case LOOKUP_OPERATOR:
                if (name.equals("AncestorLookup_Default")) {
                    sb.append(atts.getValue(Label.INPUT_TYPE)).append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else {
                        for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                            sb.append(table.get()).append(", ");
                        if (!atts.get(Label.ANCESTOR_TYPE).isEmpty()) {
                            sb.setLength(sb.length() - 2);
                        }
                    }
                }
                else if (name.equals("AncestorLookup_Nested")) {
                    if (atts.containsKey(Label.BINDING_POSITION))
                        sb.append(atts.getValue(Label.BINDING_POSITION));
                    else
                        sb.append(atts.getValue(Label.INPUT_TYPE));
                    sb.append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else {
                        for (Explainer table : atts.get(Label.ANCESTOR_TYPE))
                            sb.append(table.get()).append(", ");
                        if (!atts.get(Label.ANCESTOR_TYPE).isEmpty()) {
                            sb.setLength(sb.length() - 2);
                        }
                    }
                }
                else if (name.equals("BranchLookup_Default")) {
                    sb.append(atts.getValue(Label.INPUT_TYPE)).append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else
                        sb.append(atts.getValue(Label.OUTPUT_TYPE));
                    sb.append(" (via ").append(atts.getValue(Label.ANCESTOR_TYPE)).append(')');
                }
                else if (name.equals("BranchLookup_Nested")) {
                    if (atts.containsKey(Label.BINDING_POSITION))
                        sb.append(atts.getValue(Label.BINDING_POSITION));
                    else
                        sb.append(atts.getValue(Label.INPUT_TYPE));
                    sb.append(" -> ");
                    if (atts.containsKey(Label.TABLE_CORRELATION)) {
                        for (Explainer table : atts.get(Label.TABLE_CORRELATION))
                            sb.append(table.get()).append(", ");
                        sb.setLength(sb.length()-2);
                    }
                    else
                        sb.append(atts.getValue(Label.OUTPUT_TYPE));
                    sb.append(" (via ").append(atts.getValue(Label.ANCESTOR_TYPE)).append(")");
                }
                break;
            case COUNT_OPERATOR:
                sb.append("*");
                if (name.equals("Count_TableStatus")) {
                    sb.append(" FROM ");
                    append(atts.getAttribute(Label.INPUT_TYPE));
                }
                break;
            case FILTER:
                for (Explainer table : atts.get(Label.KEEP_TYPE)) {
                    append(table);
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
                break;
            case FLATTEN_OPERATOR:
                append(atts.getAttribute(Label.PARENT_TYPE));
                sb.append(" ").append(atts.getValue(Label.JOIN_OPTION)).append(" ");
                append(atts.getAttribute(Label.CHILD_TYPE));
                break;
            case ORDERED:
                sb.append("skip ");
                append(atts.getAttribute(Label.LEFT));
                sb.append(" left, skip ");
                append(atts.getAttribute(Label.RIGHT));
                sb.append(" right, compare ");
                append(atts.getAttribute(Label.NUM_COMPARE));
                if (name.equals("HKeyUnion")) {
                    sb.append(", shorten to ");
                    append(atts.getAttribute(Label.OUTPUT_TYPE));
                }
                else if (name.equals("Intersect_Ordered")) {
                    String join = (String)atts.getValue(Label.JOIN_OPTION);
                    if (!"INNER".equals(join)) {
                        sb.append(", USING ").append(join);
                    }
                }
                break;
            case IF_EMPTY:
                for (Explainer expression : atts.get(Label.OPERAND)) {
                    append(expression);
                    sb.append(", ");
                }
                if (!atts.valuePairs().isEmpty()) {
                    sb.setLength(sb.length() - 2);
                }
                break;
            case LIMIT_OPERATOR:
                append(atts.getAttribute(Label.LIMIT));
                break;
            case NESTED_LOOPS:
                if (name.equals("Map_NestedLoops")) {
                    if(atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append(atts.getValue(Label.TABLE_CORRELATION));
                    else
                        sb.append("loop_").append(atts.getValue(Label.BINDING_POSITION));
                }
                else if (name.equals("Product_NestedLoops")) {
                    append(atts.getAttribute(Label.OUTER_TYPE));
                    sb.append(" x ");
                    append(atts.getAttribute(Label.INNER_TYPE));
                }
                break;
            case SORT:
                int i = 0;
                for (Explainer ex : atts.get(Label.EXPRESSIONS)) {
                    append(ex);
                    sb.append(' ').append(atts.get(Label.ORDERING).get(i++).get()).append(", ");
                }
                if (atts.containsKey(Label.LIMIT)) {
                    sb.append("LIMIT ").append(atts.getValue(Label.LIMIT)).append(", ");
                }
                sb.append(atts.getValue(Label.SORT_OPTION));
                break;
            case DUI:
                if (name.equals("Delete_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("FROM ").append(atts.getValue(Label.TABLE_TYPE));
                }
                else if (name.equals("Insert_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append("INTO ").append(atts.getValue(Label.TABLE_TYPE));
                    if (atts.containsKey(Label.COLUMN_NAME)) {
                        sb.append('(');
                        for (Explainer ex : atts.get(Label.COLUMN_NAME))
                            sb.append(ex.get()).append(", ");
                        sb.setLength(sb.length()-2);
                        sb.append(')');
                    }
                }
                else if (name.equals("Update_Default")) {
                    if (atts.containsKey(Label.TABLE_CORRELATION))
                        sb.append(atts.getValue(Label.TABLE_CORRELATION));
                    else if (atts.containsKey(Label.TABLE_TYPE))
                        sb.append(atts.getValue(Label.TABLE_TYPE));
                    if (atts.containsKey(Label.COLUMN_NAME)) {
                        sb.append(" SET ");
                        for (int j = 0; j < Math.min(atts.get(Label.COLUMN_NAME).size(), atts.get(Label.EXPRESSIONS).size()); j++) {
                            sb.append(atts.get(Label.COLUMN_NAME).get(j).get()).append(" = ");
                            append(atts.get(Label.EXPRESSIONS).get(j));
                            sb.append(", ");
                        }
                        sb.setLength(sb.length()-2);
                    }
                }
                break;
            case AGGREGATE:
                if (atts.containsKey(Label.GROUPING_OPTION))
                    sb.append(atts.getValue(Label.GROUPING_OPTION)).append(": ");
                for (Explainer ex : atts.get(Label.AGGREGATORS)) {
                    sb.append(ex.get());
                    sb.append(", ");
                }
                sb.setLength(sb.length()-2);
                break;
            case BLOOM_FILTER:
                if (name.equals("Select_BloomFilter") && atts.containsKey(Label.BLOOM_FILTER)) {
                    sb.append(atts.getValue(Label.BLOOM_FILTER));
                }
                else if (name.equals("Using_BloomFilter")) {
                    sb.append(atts.getValue(Label.BINDING_POSITION));
                    if (atts.containsKey(Label.EXPRESSIONS))
                        for (Explainer ex : atts.get(Label.EXPRESSIONS))
                            sb.append(", ").append(ex.get());
                }
                break;
            case DISTINCT:
            case UNION_ALL:
            case UNION:
                break;
            default:
                throw new UnsupportedOperationException("Formatter does not recognize " + type.name());
            }
            sb.append(")");
        }
        if (atts.containsKey(Label.INPUT_OPERATOR)) {
            for (Explainer input : atts.get(Label.INPUT_OPERATOR)) {
                newRow();
                for (int i = 0; i <= depth; i++) {
                    sb.append("  ");
                }
                appendOperator((CompoundExplainer) input, depth + 1);
            }
        }
    }
    
    protected void appendRowType(CompoundExplainer opEx) {
        Attributes atts = opEx.get();
        if (atts.containsKey(Label.PARENT_TYPE) && 
            atts.containsKey((Label.CHILD_TYPE))) {
            append(atts.getAttribute(Label.PARENT_TYPE));
            sb.append(" - ");
            append(atts.getAttribute(Label.CHILD_TYPE));
        }
        else if (atts.containsKey(Label.LEFT_TYPE) && 
                 atts.containsKey((Label.RIGHT_TYPE))) {
            append(atts.getAttribute(Label.LEFT_TYPE));
            sb.append(" x ");
            append(atts.getAttribute(Label.RIGHT_TYPE));
        }
        else if (atts.containsKey(Label.TABLE_NAME)) {
            appendTableName(atts);
        }
        else {
            sb.append(atts.getValue(Label.NAME));
        }
    }

    protected void appendTableName(Attributes atts) {
        if (atts.containsKey(Label.TABLE_SCHEMA)) {
            String name = atts.getValue(Label.TABLE_SCHEMA).toString();
            if (!name.equals(defaultSchemaName))
                sb.append(name).append(".");
        }
        sb.append(atts.getValue(Label.TABLE_NAME));
    }

    protected void newRow() {
        rows.add(sb.toString());
        sb.delete(0, sb.length());
    }
}
