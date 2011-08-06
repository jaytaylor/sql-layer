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

package com.akiban.server.error;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.akiban.server.error.ErrorCode;

public final class ErrorCodeTest {
    @Test(expected=RuntimeException.class)
    public void groupTooLow() {
        ErrorCode.computeShort(-1, 0);
    }

    @Test(expected=RuntimeException.class)
    public void groupTooHigh() {
        ErrorCode.computeShort(32, 0);
    }

    @Test(expected=RuntimeException.class)
    public void subCodeTooLow() {
        ErrorCode.computeShort(-1, 0);
    }

    @Test(expected=RuntimeException.class)
    public void subCodeTooHigh() {
        ErrorCode.computeShort(1000, 0);
    }

    @Test
    public void validCodes() {
        test(0, 0, 0);
        test(1, 1, 1001);
        test(31, 987, 31987);
    }

    @Test
    public void errorCodesAllUnique() {
        final Map<Short,ErrorCode> map = new HashMap<Short, ErrorCode>(ErrorCode.values().length);
        for (ErrorCode errorCode : ErrorCode.values()) {
            ErrorCode oldCode = map.put(errorCode.getShort(), errorCode);
            if (oldCode != null) {
                fail(String.format("Conflict between codes %s and %s; both equal %d",
                        oldCode, errorCode, errorCode.getShort()));
            }
        }
    }
    
    @Test
    public void errorExceptionsUnique() {
        final Map<Class<? extends InvalidOperationException>, ErrorCode> map = new HashMap<Class<? extends InvalidOperationException>,ErrorCode >(ErrorCode.values().length);
        
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
    
    private static void test(int groupCode, int subCode, int expected) {
        assertEquals(
                String.format("ErrorCode.computeShort(%d, %d)", groupCode, subCode),
                expected,
                ErrorCode.computeShort(groupCode, subCode));
    }
}
