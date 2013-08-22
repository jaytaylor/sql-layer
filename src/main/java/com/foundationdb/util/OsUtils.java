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

package com.foundationdb.util;

import java.lang.management.ManagementFactory;

public class OsUtils {
    
    /**
     * @return the process ID of the current JVM
     */
    public static String getProcessID() {
        String pidHost = ManagementFactory.getRuntimeMXBean().getName();
        /*
         * string return from getName will always be of format
         * pid@hostname
         * tested to work on both Sun 6 and OpenJDK 6
         */
        int pidLocation = 0;
        return pidHost.split("@")[pidLocation];
    }

}
