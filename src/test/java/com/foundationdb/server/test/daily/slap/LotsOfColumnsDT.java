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
package com.foundationdb.server.test.daily.slap;

import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.daily.DailyBase;

public class LotsOfColumnsDT extends DailyBase {
    private final static int COLUMN_COUNT = 10000;

    @Test
    public void testIntColumns() {
        runTest("INT", "test_1");
    }
    
    @Test
    public void testCharColumns() {
        runTest("CHAR(1)", "table_2");
    }
    
    @Test
    public void testDoubleColumns() {
        runTest("DOUBLE", "table_3");
    }
    
    @Test
    public void testVarcharSmallColumns() {
        runTest("VARCHAR(5)", "table_4");
    }
    
    @Test
    public void testVarcharLargeColumns() {
        runTest("VARCHAR(300)", "table_5");
    }
    
    @Test
    public void testDecimalSmall() {
        runTest ("DECIMAL(3,2)", "table_6");
    }

    @Test
    public void testDecimalLarge() {
        runTest("DECIMAL(31, 21)", "table_7");
    }
    
    private void runTest (String type, String tableName) {
        StringBuilder query = new StringBuilder(COLUMN_COUNT * 32);
        
        generateColumnList (type, query);
        query.append("ID INT NOT NULL PRIMARY KEY");
        createTable("test", tableName, query.toString());

        ddl().dropTable(session(), new TableName("test", tableName));
    }

    private void generateColumnList (String type, StringBuilder query) {
        for (int i = 0; i < COLUMN_COUNT; i++) {
            query.append(String.format("COLUMN_%05d %s NOT NULL,", i, type));
        }
        
    }
}
