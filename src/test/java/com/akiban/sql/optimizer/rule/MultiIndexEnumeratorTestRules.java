/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.Duplicatable;
import com.akiban.sql.optimizer.plan.DuplicateMap;
import com.akiban.sql.optimizer.plan.IndexScan;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.PlanContext.DefaultWhiteboardMarker;
import com.akiban.sql.optimizer.plan.PlanContext.WhiteboardMarker;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.optimizer.plan.PlanWithInput;
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
            Collections.sort(scanDescriptions);
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
