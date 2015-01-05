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

package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.types.common.types.TypesTranslatorTest;

import org.junit.Test;

public class MTypesTranslatorTest extends TypesTranslatorTest
{
    public MTypesTranslatorTest() {
        super(MTypesTranslator.INSTANCE);
    }

    @Test
    public void testTypesTranslator() throws Exception {
        testType("INTEGER", "MCOMPAT_ INT(11)");
        testType("TINYINT", "MCOMPAT_ TINYINT(5)");
        testType("SMALLINT", "MCOMPAT_ SMALLINT(7)");
        testType("MEDIUMINT", "MCOMPAT_ MEDIUMINT(9)");
        testType("BIGINT", "MCOMPAT_ BIGINT(21)");

        testType("INTEGER UNSIGNED", "MCOMPAT_ INT UNSIGNED(10)");
        testType("TINYINT UNSIGNED", "MCOMPAT_ TINYINT UNSIGNED(4)");
        testType("SMALLINT UNSIGNED", "MCOMPAT_ SMALLINT UNSIGNED(6)");
        testType("MEDIUMINT UNSIGNED", "MCOMPAT_ MEDIUMINT UNSIGNED(8)");
        testType("BIGINT UNSIGNED", "MCOMPAT_ BIGINT UNSIGNED(20)");

        testType("REAL", "MCOMPAT_ FLOAT(-1, -1)");
        testType("DOUBLE", "MCOMPAT_ DOUBLE(-1, -1)");
        testType("FLOAT", "MCOMPAT_ DOUBLE(-1, -1)");
        testType("FLOAT(10)", "MCOMPAT_ FLOAT(-1, -1)");
        testType("DOUBLE UNSIGNED", "MCOMPAT_ DOUBLE UNSIGNED(-1, -1)");

        testType("DECIMAL(4,2)", "MCOMPAT_ DECIMAL(4, 2)");
        testType("DECIMAL(8,0) UNSIGNED", "MCOMPAT_ DECIMAL UNSIGNED(8, 0)");

        testType("VARCHAR(16)", "MCOMPAT_ VARCHAR(16, UTF8, UCS_BINARY)");
        testType("VARCHAR(16) COLLATE EN_US_CI_CO", "MCOMPAT_ VARCHAR(16, UTF8, en_us_ci_co)");
        testType("CHAR(2)", "MCOMPAT_ CHAR(2, UTF8, UCS_BINARY)");

        testType("DATE", "MCOMPAT_ DATE");
        testType("TIME", "MCOMPAT_ TIME");
        testType("DATETIME", "MCOMPAT_ DATETIME");
        testType("TIMESTAMP", "MCOMPAT_ DATETIME");
        testType("YEAR", "MCOMPAT_ YEAR");

        testType("CLOB", "MCOMPAT_ LONGTEXT(2147483647, UTF8, UCS_BINARY)");
        testType("TEXT", "MCOMPAT_ TEXT(65535, UTF8, UCS_BINARY)");
        testType("TINYTEXT", "MCOMPAT_ TINYTEXT(255, UTF8, UCS_BINARY)");
        testType("MEDIUMTEXT", "MCOMPAT_ MEDIUMTEXT(16777215, UTF8, UCS_BINARY)");
        testType("LONGTEXT", "MCOMPAT_ LONGTEXT(2147483647, UTF8, UCS_BINARY)");

        testType("BLOB", "MCOMPAT_ BLOB(65535)");
        testType("TINYBLOB", "MCOMPAT_ TINYBLOB(255)");
        testType("MEDIUMBLOB", "MCOMPAT_ MEDIUMBLOB(16777215)");
        testType("LONGBLOB", "MCOMPAT_ LONGBLOB(2147483647)");

        testType("BOOLEAN", "AKSQL_ BOOLEAN");
    }
    
}
