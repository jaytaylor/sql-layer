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

import com.foundationdb.sql.optimizer.plan.Duplicatable;
import com.foundationdb.sql.optimizer.plan.DuplicateMap;
import com.foundationdb.sql.optimizer.plan.IndexScan;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanToString;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;
import com.foundationdb.sql.optimizer.plan.PlanWithInput;
import com.foundationdb.sql.optimizer.rule.PlanContext.DefaultWhiteboardMarker;
import com.foundationdb.sql.optimizer.rule.PlanContext.WhiteboardMarker;
import com.foundationdb.sql.optimizer.rule.join_enum.GroupIndexGoalHooks;
import com.foundationdb.util.Strings;
import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
            List<String> scanDescriptions = new ArrayList<>(intersections.indexScans.size());
            for (IndexScan intersection : intersections.indexScans) {
                String str = intersection.summaryString(true, PlanNode.SummaryConfiguration.DEFAULT);
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
                public String summaryString(SummaryConfiguration configuration) {
                    return summary;
                }

                @Override
                public String planString(SummaryConfiguration configuration) {
                    return PlanToString.of(this, configuration);
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
        private List<IndexScan> indexScans = new ArrayList<>();
        @Override
        public Void apply(IndexScan input) {
            indexScans.add(input);
            return null;
        }
    }
}
