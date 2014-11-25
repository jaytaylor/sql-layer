/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.util;

public class Debug
{
    public static final String DEBUG_PROPERTY = "fdbsql.debug";

    public static boolean isOn() {
        return Boolean.getBoolean(DEBUG_PROPERTY);
    }

    public static boolean isOn(String subProperty) {
        String subValue = System.getProperty(DEBUG_PROPERTY + "." + subProperty);
        if(subValue != null) {
            return Boolean.parseBoolean(subValue);
        }
        return isOn();
    }
}
