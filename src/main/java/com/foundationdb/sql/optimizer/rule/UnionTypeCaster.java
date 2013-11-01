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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.expressions.TypesRegistryService;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.optimizer.TypesTranslation;
import com.foundationdb.sql.optimizer.plan.BasePlanWithInput;
import com.foundationdb.sql.optimizer.plan.CastExpression;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;
import com.foundationdb.sql.optimizer.plan.Project;
import com.foundationdb.sql.optimizer.plan.ResultSet;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.plan.Union;
import com.foundationdb.sql.optimizer.rule.ConstantFolder.NewFolder;
import com.foundationdb.sql.optimizer.rule.OverloadAndTInstanceResolver.ParametersSync;
import com.foundationdb.sql.optimizer.rule.OverloadAndTInstanceResolver.ResolvingVisitor;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

public class UnionTypeCaster extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(UnionTypeCaster.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        
        NewFolder folder = new NewFolder(plan);
        ResolvingVisitor resolvingVisitor = new ResolvingVisitor(plan, folder);
        folder.initResolvingVisitor(resolvingVisitor);
        SchemaRulesContext src = (SchemaRulesContext)plan.getRulesContext();
        TypesRegistryService registry = src.getT3Registry();
        ParametersSync parametersSync = new ParametersSync(registry.getCastsResolver());

        
        List<Union> unions = new UnionFinder().find(plan.getPlan());
        Collections.reverse(unions);
        
        for (Union union : unions) {
            updateUnion(union, folder, parametersSync);
        }
    }
    
    private class UnionFinder implements PlanVisitor {
        List<Union> result = new ArrayList<>();

        public List<Union> find(PlanNode root) {
            root.accept(this);
            return result;
        }
        
        @Override
        public boolean visitEnter(PlanNode n) {
            return visit (n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof Union) {
                Union f = (Union)n;
                    result.add(f);
            }
            return true;
        }
    }
    
    protected void updateUnion (Union union, NewFolder folder, ParametersSync parameterSync) {
        Project leftProject = getProject(union.getLeft());
        Project rightProject= getProject(union.getRight());
        Project topProject = (Project)union.getOutput();
        
        ResultSet leftResult = (ResultSet)leftProject.getOutput();
        ResultSet rightResult = (ResultSet)rightProject.getOutput();

        List<ResultField> fields = new ArrayList<> (leftProject.nFields());
        
        for (int i= 0; i < leftProject.nFields(); i++) {
            ExpressionNode leftExpr = leftProject.getFields().get(i);
            ExpressionNode rightExpr= rightProject.getFields().get(i);
            DataTypeDescriptor leftType = leftExpr.getSQLtype();
            DataTypeDescriptor rightType = rightExpr.getSQLtype();
            
            DataTypeDescriptor projectType = null;
            // Case of SELECT null UNION SELECT null -> pick a type
            if (leftType == null && rightType == null)
                projectType = new DataTypeDescriptor (TypeId.VARCHAR_ID, true);
            if (leftType == null)
                projectType = rightType;
            else if (rightType == null) 
                projectType = leftType;
            else {
                try {
                    projectType = leftType.getDominantType(rightType);
                } catch (StandardException e) {
                    projectType = null;
                }
            }
            ValueNode leftSource = leftExpr.getSQLsource();
            ValueNode rightSource = rightExpr.getSQLsource();

            CastExpression leftCast = new CastExpression(leftExpr, projectType, leftSource);
            castProjectField(leftCast, folder, parameterSync);
            leftProject.getFields().set(i, leftCast);
            

            CastExpression rightCast = new CastExpression (rightExpr, projectType, rightSource);
            castProjectField(rightCast, folder, parameterSync);
            rightProject.getFields().set(i, rightCast);
        
            ResultField leftField = leftResult.getFields().get(i);
            ResultField rightField = rightResult.getFields().get(i);
            String name = null;
            if (leftField.getName() != null && rightField.getName() != null)
                name = leftField.getName();
            else if (leftField.getName() != null)
                name = leftField.getName();
            else if (rightField.getName() != null)
                name = rightField.getName();
            
            Column column = null;
            // If both side of the union reference the same column, use it, else null
            if (leftField.getColumn() != null && rightField.getColumn() != null &&
                    leftField.getColumn() == rightField.getColumn())
                column = leftField.getColumn();
            
            fields.add(new ResultField(name, projectType, column));
            fields.get(i).setTInstance(TypesTranslation.toTInstance(projectType));
        }

        // Union -> project -> ResultSet
        if (union.getOutput().getOutput() instanceof ResultSet) {
            ResultSet rs = (ResultSet)union.getOutput().getOutput();
            ResultSet newSet = new ResultSet (union, fields);
            rs.getOutput().replaceInput(rs, newSet);
        }
    }
    
    private void castProjectField (CastExpression cast, NewFolder folder, ParametersSync parameterSync) {
        DataTypeDescriptor dtd = cast.getSQLtype();
        TInstance instance = TypesTranslation.toTInstance(dtd);
        cast.setPreptimeValue(new TPreptimeValue(instance));
        OverloadAndTInstanceResolver.finishCast(cast, folder, parameterSync);
    }
    
    protected Project getProject(PlanNode node) {
        PlanNode project = ((BasePlanWithInput)node).getInput();
        if (project instanceof Project)
            return (Project)project;
        else if (project instanceof Union) {
            Union union = (Union)project;
            project = getProject(((Union)project).getLeft());
            Project oldProject = (Project)project;
            // Add a project on top of the (nested) union 
            // to make sure the casts work on the way up
            
            Project unionProject = (Project) project.duplicate();
            unionProject.replaceInput(oldProject.getInput(), union);
            return unionProject;
        }
        else if (!(project instanceof BasePlanWithInput)) 
            return null;
        project = ((BasePlanWithInput)project).getInput();
        if (project instanceof Project)
            return (Project)project;
        return null;
    }
    
}
