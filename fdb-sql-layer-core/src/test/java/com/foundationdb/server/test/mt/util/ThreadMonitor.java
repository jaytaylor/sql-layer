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

package com.foundationdb.server.test.mt.util;

public interface ThreadMonitor
{
    enum Stage
    {
        START,
        PRE_BEGIN,
        POST_BEGIN,
        PRE_SCAN,
        SCAN_FIRST_ROW,
        SCAN_SECOND_ROW,
        POST_SCAN,
        PRE_COMMIT,
        POST_COMMIT,
        PRE_ROLLBACK,
        POST_ROLLBACK,
        FINISH
    }

    void at(Stage stage) throws Exception;
}
