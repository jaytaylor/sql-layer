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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.exec.Plannable;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Format;

import java.util.*;

/** Physical operator plan */
public abstract class BasePlannable extends BasePlanNode
{
    private Plannable plannable;
    private DataTypeDescriptor[] parameterTypes;
    private Map<Object, Explainer> extraInfo;
    
    protected BasePlannable(Plannable plannable,
                            DataTypeDescriptor[] parameterTypes) {
        this.plannable = plannable;
        this.parameterTypes = parameterTypes;
    }

    public Plannable getPlannable() {
        return plannable;
    }
    public DataTypeDescriptor[] getParameterTypes() {
        return parameterTypes;
    }

    public abstract boolean isUpdate();

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy operators.
    }
    
    public void giveInfo(Map map) {
        extraInfo = map;
    }
    
    public boolean hasInfo() {
        if (null != extraInfo)
            return !extraInfo.isEmpty();
        else return false;
    }
    
    public Map getInfo() {
        return extraInfo;
    }
    
    public List<String> explainPlan() {
        List<String> result = new ArrayList<String>();
        explainPlan(plannable, result, extraInfo);
        return result;
    }

    protected static void explainPlan(Plannable operator,
                                      List<String> into, Map<Object, Explainer> extraInfo) {
        Format f = new Format(true);
        for (String row : f.Describe(operator.getExplainer(extraInfo)))
            into.add(row);
    }
    
    @Override
    public String summaryString() {
        return withIndentedExplain(new StringBuilder(super.summaryString()));
    }

    @Override
    public String toString() {
        return withIndentedExplain(new StringBuilder(getClass().getSimpleName()));
    }

    protected String withIndentedExplain(StringBuilder str) {
        for (String operator : explainPlan()) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }

}
