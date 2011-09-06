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
import static junit.framework.Assert.*;

import java.io.File;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ViewTest extends OptimizerTestBase
{
    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "view");

    @Parameters
    public static Collection<Object[]> statements() throws Exception {
        return sqlAndExpected(RESOURCE_DIR);
    }

    public ViewTest(String caseName, String sql, String expected, String error) {
        super(caseName, sql, expected, error);
    }

    @Before
    public void loadDDL() throws Exception {
        loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
        loadView(new File(RESOURCE_DIR, "view-1.ddl"));
    }

    @Test
    public void testView() throws Exception {
        String result = null;
        Exception errorResult = null;
        try {
            StatementNode stmt = parser.parseStatement(sql);
            binder.bind(stmt);
            result = getTree(stmt);
        }
        catch (Exception ex) {
            errorResult = ex;
        }
        if (error != null) {
            if (errorResult == null)
                fail(caseName + ": error expected but none thrown");
            else
                assertEquals(caseName, error, errorResult.toString());
        }
        else if (errorResult != null) {
            throw errorResult;
        }
        else {
            assertEqualsWithoutHashes(caseName, expected, result);
        }
    }

}
