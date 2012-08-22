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

import com.akiban.qp.exec.Plannable;
import com.akiban.sql.optimizer.rule.RulesContext;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.expression.Expression;

import java.util.Map;
import java.util.HashMap;

/** A plan and its common context while running rules. */
// TODO: Consider extending this to a inheritance tree of Scenarios
// to allow exploring alternatives efficiently.
public class PlanContext
{
    private RulesContext rulesContext;
    private PlanNode plan;

    public PlanContext(RulesContext rulesContext) {
        this.rulesContext = rulesContext;
    }
                       
    public PlanContext(RulesContext rulesContext, PlanNode plan) {
        this.rulesContext = rulesContext;
        this.plan = plan;
    }
                       
    public RulesContext getRulesContext () {
        return rulesContext;
    }

    public PlanNode getPlan() {
        return plan;
    }
    public void setPlan(PlanNode plan) {
        this.plan = plan;
    }
    
    public void accept(PlanVisitor visitor) {
        plan.accept(visitor);
    }
    
    /** Type safe tag for storing objects on the context whiteboard. */
    public interface WhiteboardMarker<T> {
    }

    /** A marker class if no other conveniently unique object exists. */
    public static final class DefaultWhiteboardMarker<T> implements WhiteboardMarker<T> {
        // poor man's substitute for diamond operator
        public static <T> WhiteboardMarker<T> create() {
            return new DefaultWhiteboardMarker<T>();
        }
    }

    private Map<WhiteboardMarker<?>,Object> whiteboard = 
        new HashMap<WhiteboardMarker<?>,Object>();

    /** Store information associated with the plan for use by more
     * than one rule, but not associated directly with any part of the
     * plan tree.
     */
    public <T> T getWhiteboard(WhiteboardMarker<T> marker) {
        return (T)whiteboard.get(marker);
    }
    public <T> void putWhiteboard(WhiteboardMarker<T> marker, T value) {
        whiteboard.put(marker, value);
    }

    /** Get a {@link QueryContext} for evaluations performed during
     * compilation, issuing warnings, etc.
     */
    public QueryContext getQueryContext() {
        return new SimpleQueryContext(null);
    }

    public ExplainContext getExplainContext() {
        return null;
    }
}
