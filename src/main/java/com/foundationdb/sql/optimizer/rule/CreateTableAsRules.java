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

import com.foundationdb.sql.optimizer.plan.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** {@Create Table As Rules} takes in a planContext then visits all nodes that are
 * instances of TableSource and replaces them with CreateAs plan, these are used
 * later on to put EmitBoundRow_Nexted operators which will be used for insertion
 * and deletion from an online Create Table As query*/

/**
 * TODO in future versions this could take a specified table name or id
 * then only change these tablesources, this would be necessary if in future versions
 * we accept queries with union, intersect, except, join, etc
 */
 public class CreateTableAsRules extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(SortSplitter.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {

        Results results =  new CreateTableAsFinder().find(plan.getPlan());
        CreateAs createAs = null;
        TableNode firstTable = null;
        if(!results.tables.isEmpty()){
            firstTable = results.tables.get(0).getTable();
        }//TODO could this be achieved by asserting size is <=1 or are there complex queries that could cause multiple Tablesources with same table
        for (TableSource tableSource : results.tables) {
            assert(tableSource.getTable().equals(firstTable));
            createAs = transform(tableSource);
        }
        assert(createAs != null);
        for (Project project : results.projects) {
            transform(project, createAs);
        }
    }

    protected CreateAs transform(TableSource tableSource) {
        CreateAs createAs = new CreateAs();
        createAs.setOutput(tableSource.getOutput());
        (tableSource.getOutput()).replaceInput(tableSource, createAs);
        createAs.setTableSource(tableSource);
        return createAs;
    }//replace each instance of the tableSource  with a createAs

    protected void transform(Project project, CreateAs createAs){
        for (int i = 0; i < project.getFields().size(); i++){
            if(project.getFields().get(i) instanceof ColumnExpression) {
                ColumnExpression expression = (ColumnExpression) project.getFields().get(i);
                project.getFields().remove(i);
                project.getFields().add(i, new ColumnExpression(expression, createAs));
            }
            //fix column or table???
        }
    }
    static class Results

    {
        public List<TableSource> tables = new ArrayList<>();
        public List<Project> projects = new ArrayList<>();
    }


    static class CreateTableAsFinder implements PlanVisitor, ExpressionVisitor {

        Results results;

        public Results find(PlanNode root) {
            results = new Results();
            root.accept(this);
            return results;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof TableSource)
                results.tables.add((TableSource)n);
            else if (n instanceof Project){
                results.projects.add((Project) n);
            }
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            return true;
        }
    }

}