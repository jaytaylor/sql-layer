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

package com.akiban.cserver;

public interface CServerConstants {
//    public final static short OK = 1;
//    public final static short END = 2;
//    public final static short ERR = 100;
//    public final static short MISSING_OR_CORRUPT_ROW_DEF = 99;
//    public final static short UNSUPPORTED_MODIFICATION = 98;
//    public final static short NOT_REALLY_AN_ERROR_ERROR = 42; //TODO
    
    public final static int DEFAULT_CSERVER_PORT = 5140;
    public final static String DEFAULT_CSERVER_HOST_STRING = "0.0.0.0";
    public final static String DEFAULT_CSERVER_PORT_STRING = Integer.toString(DEFAULT_CSERVER_PORT);

    public final static int MAX_VERSIONS_PER_TABLE = 65536;
    public final static int MAX_GROUP_DEPTH = 256;

}
