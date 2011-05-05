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

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.StatementNode;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SubqueryFlattenerTest extends OptimizerTestBase
{
  public static final File RESOURCE_DIR = 
    new File(OptimizerTestBase.RESOURCE_DIR, "flatten");

  @Parameters
  public static Collection<Object[]> statements() throws Exception {
    return sqlAndExpected(RESOURCE_DIR);
  }

  public SubqueryFlattenerTest(String caseName, String sql, String expected) {
    super(caseName, sql, expected);
  }

  @Before
  public void loadDDL() throws Exception {
    loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
    loadView(new File(RESOURCE_DIR, "view-1.ddl"));
    ((BoundNodeToString)unparser).setUseBindings(true);
  }

  @Test
  public void testFlatten() throws Exception {
    StatementNode stmt = parser.parseStatement(sql);
    binder.bind(stmt);
    stmt = booleanNormalizer.normalize(stmt);
    typeComputer.compute(stmt);
    stmt = subqueryFlattener.flatten(stmt);
    assertEquals(expected.trim(), unparser.toString(stmt));
  }

}
