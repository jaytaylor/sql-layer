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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.Duplicatable;
import com.akiban.sql.optimizer.plan.DuplicateMap;
import com.akiban.sql.optimizer.plan.IndexScan;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.optimizer.plan.PlanWithInput;
import com.akiban.sql.optimizer.rule.PlanContext;
import com.akiban.sql.optimizer.rule.PlanContext.DefaultWhiteboardMarker;
import com.akiban.sql.optimizer.rule.PlanContext.WhiteboardMarker;
import com.akiban.sql.optimizer.rule.join_enum.GroupIndexGoalHooks;
import com.akiban.util.Strings;
import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused") // created by reflection from rules.yml
public final class MultiIndexEnumeratorTestRules {
    private static Logger logger = LoggerFactory.getLogger(MultiIndexEnumeratorTestRules.class);
    private static WhiteboardMarker<IntersectionViewer> intersectionViewerMarker = DefaultWhiteboardMarker.create();
    
    public static class InstallHooks extends BaseRule {
        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        public void apply(PlanContext plan) {
            IntersectionViewer hook = new IntersectionViewer();
            plan.putWhiteboard(intersectionViewerMarker, hook);
            GroupIndexGoalHooks.hookIntersectedIndexes(hook);
        }
    }
    
    public static class ResultHooks extends BaseRule {
        @Override
        protected Logger getLogger() {
            return logger;
        }

        @Override
        public void apply(PlanContext plan) {
            GroupIndexGoalHooks.unhookIntersectedIndexes();
            plan.setPlan(useHooks(plan.getPlan(), plan.getWhiteboard(intersectionViewerMarker)));
        }

        private PlanNode useHooks(final PlanNode plan, IntersectionViewer intersections) {
            List<String> scanDescriptions = new ArrayList<String>(intersections.indexScans.size());
            for (IndexScan intersection : intersections.indexScans) {
                String str = intersection.summaryString(true);
                scanDescriptions.add(str);
            }
            final String summary = Strings.join(scanDescriptions);
            return new PlanNode() {
                @Override
                public PlanWithInput getOutput() {
                    return plan.getOutput();
                }

                @Override
                public void setOutput(PlanWithInput output) {
                    plan.setOutput(output);
                }

                @Override
                public boolean accept(PlanVisitor v) {
                    v.visit(this);
                    return true;
                }

                @Override
                public String summaryString() {
                    return summary;
                }

                @Override
                public Duplicatable duplicate() {
                    return plan.duplicate();
                }

                @Override
                public Duplicatable duplicate(DuplicateMap map) {
                    return plan.duplicate(map);
                }
            };
        }
    }
    
    private static class IntersectionViewer implements Function<IndexScan,Void> {
        private List<IndexScan> indexScans = new ArrayList<IndexScan>();
        @Override
        public Void apply(IndexScan input) {
            indexScans.add(input);
            return null;
        }
    }
}
