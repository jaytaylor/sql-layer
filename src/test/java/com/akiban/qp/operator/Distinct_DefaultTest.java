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

package com.akiban.qp.operator;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.OperatorTestHelper;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import static com.akiban.qp.operator.API.*;

import com.akiban.server.types.AkType;

import org.junit.Test;

import java.util.Arrays;
import java.util.Deque;

public class Distinct_DefaultTest {

    @Test
    public void testDistinct() {
        Operator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.LONG)
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",1L)
            .row(3L,"def",0L)
            .row(3L,"def",(Long)null)
            .row(1L,"abc",0L)
        );
        Operator plan = distinct_Default(input, input.rowType());
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.LONG)
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",1L)
            .row(3L,"def",0L)
            .row(3L,"def",(Long)null)
            .row(1L,"abc",0L)
            .rows();
        OperatorTestHelper.check(plan, expected);
    }

}
