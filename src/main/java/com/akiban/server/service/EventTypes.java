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

package com.akiban.server.service;

public class EventTypes {
    
    public static final String PROCESS = "sql: process";
    
    public static final String PARSE = "sql: parse";
    
    public static final String OPTIMIZE = "sql: optimize";
    
    public static final String COMPILE = "sql: optimize: compile";
    
    public static final String BIND_AND_GROUP = "sql: optimize: bindandgroup";
    
    public static final String PICK_BEST_INDEX = "sql: optimize: pickbestindex";
    
    public static final String FLATTEN = "sql: optimize: flatten";
    
    public static final String EXECUTE = "sql: execute";

}
