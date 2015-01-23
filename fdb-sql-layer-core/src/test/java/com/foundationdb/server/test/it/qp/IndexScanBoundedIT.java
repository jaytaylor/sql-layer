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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * This test covers bounded index scans with combinations of the following variations:
 * - ascending/descending/mixed order
 * - inclusive/exclusive/semi-bounded
 * - bound is present/missing
 */

public class IndexScanBoundedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key",
            "a int",
            "b int",
            "c int");
        createIndex("schema", "t", "a", "a", "b", "c", "id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new Row[]{
            // No nulls
            row(t, 1000L, 1L, 11L, 111L),
            row(t, 1001L, 1L, 11L, 115L),
            row(t, 1002L, 1L, 15L, 151L),
            row(t, 1003L, 1L, 15L, 155L),
            row(t, 1004L, 5L, 51L, 511L),
            row(t, 1005L, 5L, 51L, 515L),
            row(t, 1006L, 5L, 55L, 551L),
            row(t, 1007L, 5L, 55L, 555L),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    // Test name: test_AB_CD
    // A: Lo Inclusive/Exclusive
    // B: Lo Bound is present/missing
    // C: Hi Inclusive/Exclusive
    // D: Hi Bound is present/missing
    // AB/CD combinations are not tested exhaustively because processing at
    // start and end of scan are independent.

    @Test
    public void test_IP_IP_AAA()
    {
        // AAA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC, ASC),
             1002, 1003);
    }

    @Test
    public void test_IP_IP_AA() 
    {
        // AA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC),
             1002, 1003);
    }
    
    @Test
    public void test_IP_IP_A()
    {
        // A
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC),
             1002, 1003);
    }
/*    
        // AAD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002, 1005, 1004, 1007, 1006);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        // AA, A already tested
        // ADA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        // AD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC),
             1002, 1003);
        // A already tested
        // ADD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        // AD, A already tested
        // DAA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        // DA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC),
             1002, 1003);
*/
    @Test
    public void test_IP_IP_D()
    {
        // D
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC),
             1003, 1002);
    }
/*        // DAD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        // DA, D already tested
        // DDA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC, ASC),
             1002, 1003);
*/
    @Test
    public void test_IP_IP_DD()
    {
        // DD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC),
             1003, 1002);
    }
    @Test
    public void test_IP_IP_DDD()
    {
        // DDD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC, DESC),
             1003, 1002);
    }

    @Test
    public void test_IM_IM_AAA()
    {
        // AAA
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(ASC, ASC, ASC),
             1002, 1003);
    }
    
    @Test
    public void test_IM_IM_AA()
    {
        // AA
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(ASC, ASC),
             1002, 1003);
    }
    
    @Test
    public void test_IM_IM_A()
    {
        // A
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(ASC),
             1002, 1003);
    }
/*    
        // AAD
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002, 1005, 1004, 1007, 1006);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        // AA, A already tested
        // ADA
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        // AD
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(ASC, DESC),
             1002, 1003);
        // A already tested
        // ADD
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        // AD, A already tested
        // DAA
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        // DA
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(DESC, ASC),
             1002, 1003);
*/
    @Test
    public void test_IM_IM_D() 
    {
        // D
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(DESC),
             1003, 1002);
    }
/*
        // DAD
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        // DA, D already tested
        // DDA
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(DESC, DESC, ASC),
             1002, 1003);
*/
    @Test
    public void test_IM_IM_DD()
    {
        // DD
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(DESC, DESC),
             1003, 1002);
    }
    
    @Test
    public void test_IM_IM_DDD() 
    {
        // DDD
        test(range(INCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 14, UNSPECIFIED,
                   INCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 149,
                   INCLUSIVE, 1, 15, 160),
             ordering(DESC, DESC, DESC),
             1003, 1002);
        // DD, D already tested
    }
    
    @Test
    public void test_EP_EP_AAA()
    {
        // AAA
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC, ASC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC, ASC));
    }
    @Test
    public void test_EP_EP_AA()
    {
        // AA
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC));
    }
    @Test
    public void test_EP_EP_A()
    {
        // A
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(ASC));
    }
/*    
        // AAD
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC, DESC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC, DESC));
        // AA, A already tested
        // ADA
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, ASC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC, ASC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC, ASC));
        // AD
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC));
        // A already tested
        // ADD
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC, DESC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC, DESC));
        // AD, A already tested
        // DAA
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC, ASC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC, ASC));
        // DA
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC));
*/
    @Test
    public void test_EP_EP_D()
    {
        // D
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(DESC));
    }
/*    
        // DAD
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC, DESC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC, DESC));
        // DA, D already tested
        // DDA
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC, ASC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC, ASC));
*/
    @Test
    public void test_EP_EP_DD()
    {
        // DD
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC));
    }
    
    @Test
    public void test_EP_EP_DDD()
    {
        // DDD
        test(range(EXCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC));
        test(range(EXCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC, DESC));
        test(range(EXCLUSIVE, 1, 15, 151,
                   EXCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC, DESC));
    }
    
    @Test
    public void test_EM_EM_AAA()
    {
        // AAA
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(ASC, ASC, ASC),
             1002, 1003);
    }
    @Test
    public void test_EM_EM_AA()
    {
        // AA
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, ASC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(ASC, ASC),
             1002, 1003);
    }
    @Test
    public void test_EM_EM_A()
    {
        // A
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(ASC),
             1002, 1003);
    }
/*    
        // AAD
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002, 1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        // AA, A already tested
        // ADA
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        // AD
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(ASC, DESC),
             1002, 1003);
        // A already tested
        // ADD
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        // AD, A already tested
        // DAA
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        // DA
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, ASC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(DESC, ASC),
             1002, 1003);
*/
    @Test
    public void test_EM_EM_D()
    {
        // D
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC),
             1003, 1002);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(DESC),
             1003, 1002);
    }
/*        // DAD
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        // DA, D already tested
        // DDA
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1002, 1003);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(DESC, DESC, ASC),
             1002, 1003);
*/
    @Test
    public void test_EM_EM_DD()
    {
        // DD
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, DESC),
             1003, 1002);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(DESC, DESC),
             1003, 1002);
    }
        // D already tested
    @Test
    public void test_EM_EM_DDD()
    {
        // DDD
        test(range(EXCLUSIVE, 0, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 6, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 1, 14, UNSPECIFIED,
                   EXCLUSIVE, 1, 16, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1003, 1002);
        test(range(EXCLUSIVE, 1, 15, 149,
                   EXCLUSIVE, 1, 15, 160),
             ordering(DESC, DESC, DESC),
             1003, 1002);
    }

    // Test half-bounded ranges

    @Test
    public void testBoundedLeftInclusive_AAA()
    {
        // AAA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(ASC, ASC, ASC),
             1000, 1001);
    }
    @Test
    public void testBoundedLeftInclusive_AA()
    {
        // AA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(ASC, ASC),
             1000, 1001);
    }
    @Test
    public void testBoundedLeftInclusive_A()
    {
        // A
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(ASC),
             1000, 1001);
    }
/*    
        // AAD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002, 1005, 1004, 1007, 1006);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(ASC, ASC, DESC),
             1001, 1000);
        // AA, A already tested
        // ADA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(ASC, DESC),
             1000, 1001);
        // AD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(ASC, DESC),
             1000, 1001);
        // A already tested
        // ADD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(ASC, DESC, DESC),
             1001, 1000);
        // AD, A already tested
        // DAA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC, ASC, ASC),
             1000, 1001);
        // DA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC, ASC),
             1000, 1001);
*/
    @Test
    public void testBoundedLeftInclusive_D()
    {
        // D
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC),
             1001, 1000);
    }
/*        // DAD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC, ASC, DESC),
             1001, 1000);
        // DA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC, ASC),
             1000, 1001);
        // D already tested
        // DDA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC, DESC, ASC),
             1000, 1001);
*/
    @Test
    public void testBoundedLeftInclusive_DD()
    {
        // DD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC, DESC),
             1001, 1000);
        // D already tested
    }
    @Test
    public void testBoundedLeftInclusive_DDD()
    {
        // DDD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   EXCLUSIVE, 1, null, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 11, 111,
                   EXCLUSIVE, 1, 11, null),
             ordering(DESC, DESC, DESC),
             1001, 1000);
        // DD, D already tested
    }

    @Test
    public void testBoundedLeftExclusive_AAA()
    {
        // AAA
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(ASC, ASC, ASC),
             1004, 1005);
    }
    @Test
    public void testBoundedLeftExclusive_AA()
    {
        // AA
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(ASC, ASC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(ASC, ASC),
             1004, 1005);
    }
    @Test
    public void testBoundedLeftExclusive_A()
    {
        // A
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(ASC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(ASC),
             1004, 1005);
    }
/*    
        // AAD
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1007, 1006);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(ASC, ASC, DESC),
             1005, 1004);
        // AA, A already tested
        // ADA
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(ASC, DESC),
             1004, 1005);
        // AD
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(ASC, DESC),
             1004, 1005);
        // A already tested
        // ADD
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1007, 1006);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(ASC, DESC, DESC),
             1005, 1004);
        // AD, A already tested
        // DAA
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC, ASC, ASC),
             1004, 1005);
        // DA
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC, ASC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC, ASC),
             1004, 1005);
*/
    @Test
    public void testBoundedLeftExclusive_D()
    {
        // D
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC),
             1007, 1006);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC),
             1005, 1004);
    }
/*        // DAD
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1007, 1006);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC, ASC, DESC),
             1005, 1004);
        // DA
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC, ASC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC, ASC),
             1004, 1005);
        // D already tested
        // DDA
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC, DESC, ASC),
             1004, 1005);
*/
    @Test
    public void testBoundedLeftExclusive_DD()
    {
        // DD
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC, DESC),
             1005, 1004);
        // D already tested
    }
    @Test
    public void testBoundedLeftExclusive_DDD()
    {
        // DDD
        test(range(EXCLUSIVE, 3, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 53, UNSPECIFIED,
                   EXCLUSIVE, 5, null, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006);
        test(range(EXCLUSIVE, 5, 51, 500,
                   EXCLUSIVE, 5, 51, null),
             ordering(DESC, DESC, DESC),
             1005, 1004);
        // DD, D already tested
    }

    @Test
    public void testBoundedRightInclusive_AAA()
    {
        // AAA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, ASC, ASC),
             1006);
    }
    @Test
    public void testBoundedRightInclusive_AA()
    {
        // AA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, ASC),
             1006);
    }
    @Test
    public void testBoundedRightInclusive_A()
    {
        // A
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC),
             1006);
    }
/*    
        // AAD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002, 1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, ASC, DESC),
             1006);
        // AA, A already tested
        // ADA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, DESC),
             1006);
        // AD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, DESC),
             1006);
        // A already tested
        // ADD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, DESC, DESC),
             1006);
        // AD, A already tested
        // DAA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC, ASC, ASC),
             1006);
        // DA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC, ASC),
             1006);
*/
    @Test
    public void testBoundedRightInclusive_D()
    {
        // D
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC),
             1006);
    }
/*    
        // DAD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC, ASC, DESC),
             1006);
        // DA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC, ASC),
             1006);
        // D already tested
        // DDA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC, DESC, ASC),
             1006);
*/
    @Test
    public void testBoundedRightInclusive_DD()
    {
        // DD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC, DESC),
             1006);
        // D already tested
    }
    @Test
    public void testBoundedRightInclusive_DDD()
    {
        // DDD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   INCLUSIVE, 5, 55, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   INCLUSIVE, 5, 55, 551),
             ordering(DESC, DESC, DESC),
             1006);
        // DD, D already tested
    }

    @Test
    public void testBoundedRightExclusive_AAA()
    {
        // AAA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(ASC, ASC, ASC),
             1006);
    }
    @Test
    public void testBoundedRightExclusive_AA()
    {
        // AA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(ASC, ASC),
             1006);
    }
    @Test
    public void testBoundedRightExclusive_A()
    {
        // A
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(ASC),
             1006);
    }
/*        // AAD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002, 1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(ASC, ASC, DESC),
             1006);
        // AA, A already tested
        // ADA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(ASC, DESC),
             1006);
        // AD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(ASC, DESC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(ASC, DESC),
             1006);
        // A already tested
        // ADD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(ASC, DESC, DESC),
             1006);
        // AD, A already tested
        // DAA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC, ASC, ASC),
             1006);
        // DA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC, ASC),
             1006);
*/
    @Test
    public void testBoundedRightExclusive_D()
    {
        // D
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC),
             1006);
    }
/*        // DAD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC, ASC, DESC),
             1006);
        // DA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC, ASC),
             1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC, ASC),
             1006);
        // D already tested
        // DDA
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC, DESC, ASC),
             1006);
*/
    @Test
    public void testBoundedRightExclusive_DD()
    {
        // DD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC, DESC),
             1006);
        // D already tested
    }
    @Test
    public void testBoundedRightExclusive_DDD()
    {
        // DDD
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   EXCLUSIVE, 8, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, 5, null, UNSPECIFIED,
                   EXCLUSIVE, 5, 58, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, 5, 55, null,
                   EXCLUSIVE, 5, 55, 553),
             ordering(DESC, DESC, DESC),
             1006);
        // DD, D already tested
    }

    // case 2: > null <= null
    @Test(expected=IllegalArgumentException.class)
    public void leftExclusiveNullRightInclusiveNull() {
        test(range(EXCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000);
    }

    // case 6: > non-null, <= null
    @Test(expected=IllegalArgumentException.class)
    public void leftExclusiveNonNullRightInclusiveNull() {
        test(range(EXCLUSIVE, 1000, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000);
    }

    // case 10: >= null, <= null
    @Test
    public void leftInclusiveNullRightInclusiveNull() {
        Row row = row(t, 2000L, null, 11L, 111L);
        writeRows(row);
        db = Arrays.copyOf(db, db.length + 1);
        db[db.length -1] = row;
        test(range(INCLUSIVE, null, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             2000);
    }

    // case 14: >= non-null, <= null
    @Test(expected=IllegalArgumentException.class)
    public void leftInclusiveNonNullRightInclusiveNull() {
        test(range(INCLUSIVE, 1000, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, null, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000);
    }

    // For use by this class

    private void test(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        Row[] expected = new Row[expectedIds.length];
        for (int i = 0; i < expectedIds.length; i++) {
            int id = expectedIds[i];
            expected[i] = dbRow(id);
        }
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private void dump(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        dumpToAssertion(plan);
    }

    private IndexKeyRange range(boolean loInclusive, Integer aLo, Integer bLo, Integer cLo,
                                boolean hiInclusive, Integer aHi, Integer bHi, Integer cHi)
    {
        IndexBound lo;
        if (aLo == UNSPECIFIED) {
            lo = null;
            fail();
        } else if (bLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo), new SetColumnSelector(0));
        } else if (cLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo, bLo), new SetColumnSelector(0, 1));
        } else {
            lo = new IndexBound(row(idxRowType, aLo, bLo, cLo), new SetColumnSelector(0, 1, 2));
        }
        IndexBound hi;
        if (aHi == UNSPECIFIED) {
            hi = null;
            fail();
        } else if (bHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi), new SetColumnSelector(0));
        } else if (cHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi, bHi), new SetColumnSelector(0, 1));
        } else {
            hi = new IndexBound(row(idxRowType, aHi, bHi, cHi), new SetColumnSelector(0, 1, 2));
        }
        return IndexKeyRange.bounded(idxRowType, lo, loInclusive, hi, hiInclusive);
    }

    private API.Ordering ordering(boolean... directions)
    {
        assertTrue(directions.length >= 1 && directions.length <= 3);
        API.Ordering ordering = API.ordering();
        if (directions.length >= 1) {
            ordering.append(field(idxRowType, A), directions[0]);
        }
        if (directions.length >= 2) {
            ordering.append(field(idxRowType, B), directions[1]);
        }
        if (directions.length >= 3) {
            ordering.append(field(idxRowType, C), directions[2]);
        }
        return ordering;
    }

    private Row dbRow(long id)
    {
        for (Row newRow : db) {
            if (ValueSources.getLong(newRow.value(0)) == id) {
                return row(idxRowType,
                           ValueSources.toObject(newRow.value(1)),
                           ValueSources.toObject(newRow.value(2)),
                           ValueSources.toObject(newRow.value(3)),
                           ValueSources.toObject(newRow.value(0)));
            }
        }
        fail();
        return null;
    }

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;
    private static final boolean EXCLUSIVE = false;
    private static final boolean INCLUSIVE = true;
    private static final Integer UNSPECIFIED = new Integer(Integer.MIN_VALUE); // Relying on == comparisons

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
}
