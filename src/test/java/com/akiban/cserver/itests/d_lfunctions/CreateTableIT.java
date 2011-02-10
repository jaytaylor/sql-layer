/*
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

package com.akiban.cserver.itests.d_lfunctions;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public final class CreateTableIT extends ApiTestBase {
    /*
     * ENUM parsing and AIS support
     * Note: ENUM unsupported for Halo
     */
    @Test(expected=InvalidOperationException.class)//(expected=UnsupportedDataTypeException.class)
    public void bug686972() throws InvalidOperationException {
        int tid = createTable("test", "t", "c1 enum('a','b','c')");
    }

    /*
     * Using 'text' as a column identifier causes parse error
     */
    @Test
    public void bug687146() throws InvalidOperationException {
        int tid = createTable("test", "t", "entry int primary key, text varchar (2000)");
    }


    /*
     * Column or table comment causes parse error
     */
    @Test
    public void bug687220() throws InvalidOperationException {
        int tid1 = createTable("test", "t1", "id int key COMMENT 'Column comment activate'");

        ddl().createTable(session, "test", "create table t2(id int key) COMMENT='A table comment'");
        int tid2 = tableId("test", "t2");
    }

    /*
     * Intitial auto increment value is incorrect
     */
    @Test
    public void bug696169() throws Exception {
        ddl().createTable(session, "test", "CREATE TABLE t(c1 INT AUTO_INCREMENT KEY) AUTO_INCREMENT=10");
        int tid = tableId("test", "t");
        
        assertEquals("auto inc is n-1",
                     9,
                     store().getTableStatistics(session, tid).getAutoIncrementValue());
    }

    /*
     * FIXED data type causes parse error
     */
    @Test
    public void bug696321() throws InvalidOperationException {
        int tid = createTable("test", "t", "c1 FIXED NULL");
    }

    /*
     * REAL data type causes NPE
     */
    @Test
    public void bug696325() throws InvalidOperationException {
        int tid = createTable("test", "t", "c1 REAL(1,0) NULL");
    }

    /*
     * Short create statement causes StringIndexOutOfBoundsException from
     * SchemaDef.canonicalStatement
     */
    @Test
    public void bug705920() throws InvalidOperationException {
        int tid = createTable("test", "t", "c1 int");
    }

    /*
     * BIT datatype is unsupported
     * Note: BIT unsupported for Halo
     */
    @Test(expected=InvalidOperationException.class)//(expected=UnsupportedDataTypeException.class)
    public void bug705980() throws InvalidOperationException {
        int tid1 = createTable("test", "t1", "c1 BIT");
        int tid2 = createTable("test", "t1", "c1 BIT(0)");  // => BIT(1)
        int tid3 = createTable("test", "t2", "c1 BIT(1)");  // Min
        int tid4 = createTable("test", "t3", "c1 BIT(64)"); // Max
        int tid5 = createTable("test", "t4", "c1 BIT(65)"); // Too big
    }
    
    /*
     * CHAR(0) data type fails, assert during AIS construction
     */
    @Test
    public void bug705993() throws InvalidOperationException {
        int tid = createTable("test", "t", "c1 CHAR(0) NULL");
    }

    /*
     * SERIAL data types are parse errors
     */
    @Test
    public void bug706008() throws InvalidOperationException {
        // SERIAL => BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
        int tid1 = createTable("test", "t", "c1 SERIAL");

        // [int type] SERIAL DEFAULT VALUE => [int type] NOT NULL AUTO_INCREMENT UNIQUE.
        int tid2 = createTable("test", "t", "c1 int SERIAL DEFAULT VALUE");
    }

    /*
     * CREATE TABLE .. LIKE .. is parse error
     */
    @Test
    public void bug706344() throws InvalidOperationException {
        int tid1 = createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 CHAR(10) NULL, PRIMARY KEY(c1)");
        ddl().createTable(session, "test", "CREATE TABLE dst LIKE src");
        int tid2 = tableId("test", "dst");
    }

    /*
     * CREATE TABLE .. SELECT .. FROM .. is parse error
     */
    public void bug706347() throws InvalidOperationException {
        int tid1 = createTable("test", "src", "c1 INT NOT NULL AUTO_INCREMENT, c2 INT NULL, PRIMARY KEY(c1))");
        ddl().createTable(session, "test", "c1 INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(c1)) SELECT c1,c2 FROM src");
        int tid2 = tableId("test", "dst");
    }
}
