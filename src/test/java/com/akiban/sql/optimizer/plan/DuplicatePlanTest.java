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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.rule.ASTStatementLoader;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collection;
import java.util.ArrayList;

@RunWith(Parameterized.class)
public class DuplicatePlanTest extends OptimizerTestBase
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "plan/duplicate");

    @Parameters
    public static Collection<Object[]> statements() throws Exception {
        Collection<Object[]> result = new ArrayList<Object[]>();
        for (File sqlFile : listSQLFiles(RESOURCE_DIR)) {
            String caseName = sqlFile.getName().replace(".sql", "");
            String sql = fileContents(sqlFile);
            result.add(new Object[] {
                           caseName, sql
                       });
        }
        return result;
    }

    public DuplicatePlanTest(String caseName, String sql) {
        super(caseName, sql, null, null);
    }

    @Before
    public void loadDDL() throws Exception {
        loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
    }

    @Test
    public void testDuplicatePlan() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        // Turn parsed AST into intermediate form.
        PlanNode plan = new AST((DMLStatementNode)stmt);
        plan = new ASTStatementLoader().apply(plan);
        PlanNode duplicate = (PlanNode)plan.duplicate();
        assertFalse(plan == duplicate);
        assertEqualsWithoutHashes(caseName, 
                                  PlanToString.of(plan), 
                                  PlanToString.of(duplicate));
    }

}
