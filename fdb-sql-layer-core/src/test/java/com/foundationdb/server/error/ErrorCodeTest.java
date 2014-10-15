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

package com.foundationdb.server.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

public final class ErrorCodeTest {

    @Test
    public void errorCodesAllUnique() {
        final Map<String,ErrorCode> map = new HashMap<>(ErrorCode.values().length);
        for (ErrorCode errorCode : ErrorCode.values()) {
            ErrorCode oldCode = map.put(errorCode.getFormattedValue(), errorCode);
            if (oldCode != null) {
                fail(String.format("Conflict between codes %s and %s; both equal %s",
                        oldCode, errorCode, errorCode.getFormattedValue()));
            }
        }
    }
    
    @Test
    public void errorExceptionsUnique() {
        final Map<Class<? extends InvalidOperationException>, ErrorCode> map = new HashMap<>(ErrorCode.values().length);
        
        for (ErrorCode errorCode : ErrorCode.values()) {
            // don't check the null ones. 
            if (errorCode.associatedExceptionClass() == null) { continue; } 
            ErrorCode oldCode = map.put (errorCode.associatedExceptionClass(), errorCode);
            if (oldCode != null) {
               fail (String.format("Duplicate Exception between %s and %s, both with %s exception" , 
                       oldCode, errorCode, errorCode.associatedExceptionClass()));
           }
        }
    }

    @Test
    public void errorHasMessage() {
         for (ErrorCode errorCode : ErrorCode.values()) {
             assertNotNull (errorCode.getMessage());
         }
    }

    @Test
    public void noExtraMessages() {
        Set<String> enumNames = new TreeSet<>();
        for(ErrorCode code : ErrorCode.values()) {
            enumNames.add(code.name());
        }
        Set<String> msgNames = new TreeSet<>(ErrorCode.resourceBundle.keySet());
        msgNames.removeAll(enumNames);
        assertEquals("[]", msgNames.toString());
    }
}
