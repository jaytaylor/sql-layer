/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.server.explain.ExplainContext;

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
            return new DefaultWhiteboardMarker<>();
        }
    }

    private Map<WhiteboardMarker<?>,Object> whiteboard = 
        new HashMap<>();

    /** Store information associated with the plan for use by more
     * than one rule, but not associated directly with any part of the
     * plan tree.
     */
    @SuppressWarnings("unchecked")
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


    /** Format a hierarchical view of the current plan. */
    public String planString() {
        return plan.planString();
    }
}
