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
 
package com.foundationdb.server.error;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

/**
 * The categories of error code classes that are part of the public API.
 *
 * From the SQL Standard, the SQLSTATE (ErrorCodes) are a 2 character class value
 * followed by a 3 character sub-class value. These characters are either digits or
 * upper-case latin characters. (0-9 or A-Z).
**/
public class ErrorCodeClass {

    static final ResourceBundle resourceBundle = ResourceBundle.getBundle("com.foundationdb.server.error.error_code_class");
    private final String key;
    private final String description;

    private ErrorCodeClass(String key, String description) {
        this.key = key;
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public String getDescription() {
        return description;
    }

    public static List<ErrorCodeClass> getClasses() {
        List<ErrorCodeClass> classes = new ArrayList<>(60);
        for (Enumeration<String> keys = resourceBundle.getKeys(); keys.hasMoreElements();) {
            String key = keys.nextElement();
            classes.add(new ErrorCodeClass(key, resourceBundle.getString(key)));
        }
        return classes;
    }

}
