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

package com.foundationdb.server.test.it.routines;

/** Basic Java stored procedures
 * <code><pre>
CALL sqlj.install_jar('target/fdb-sql-layer-x.y.z-tests.jar', 'testjar', 0);
CREATE PROCEDURE test.add_sub(IN x INT, IN y INT, OUT "sum" INT, out diff INT) LANGUAGE java PARAMETER STYLE java EXTERNAL NAME 'testjar:com.foundationdb.server.test.it.routines.TestJavaBasic.addSub';
CALL test.add_sub(100,59);
 * </pre></code> 
 */
public class TestJavaBasic
{
    public static void addSub(int x, int y, int[] sum, int[] diff) {
        sum[0] = x + y;
        diff[0] = x - y;
    }
}
