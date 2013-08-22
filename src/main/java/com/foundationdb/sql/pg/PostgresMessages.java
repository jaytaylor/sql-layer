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

package com.foundationdb.sql.pg;

import java.util.HashMap;
import java.util.Map;

public enum PostgresMessages {

    EOF_TYPE                (-1,  0, true, true, ErrorMode.NONE),  // (F&B)
    AUTHENTICATION_TYPE     ('R', 8, false, true),  // (B)
    BACKEND_KEY_DATA_TYPE   ('K', 12, false, true), // (B)
    BIND_TYPE               ('B', Integer.MAX_VALUE, true, false, ErrorMode.EXTENDED), // (F)
    BIND_COMPLETE_TYPE      ('2', 4,  false, true),  // (B)
    CLOSE_TYPE              ('C', 1024, true, false), // (F)
    CLOSE_COMPLETE_TYPE     ('3', 4,  false, true),  // (B)
    COMMAND_COMPLETE_TYPE   ('C', 128, false, true), // (B)
    COPY_DATA_TYPE          ('d', Integer.MAX_VALUE, true, true), // (F&B)
    COPY_DONE_TYPE          ('c', 4,  true, true),  // (F&B)
    COPY_FAIL_TYPE          ('f', 1024, true, true), // (F&B)
    COPY_IN_RESPONSE_TYPE   ('G', 1024, false, true), // (B) -> 508 columns
    COPY_OUT_RESPONSE_TYPE  ('H', 1024, false, true), // (B) -> 508 columns 
    COPY_BOTH_RESPONSE_TYPE ('W', 1024, false, true), // (B)
    DATA_ROW_TYPE           ('D', Integer.MAX_VALUE, false, true), // (B)
    DESCRIBE_TYPE           ('D', 1024, true, false, ErrorMode.EXTENDED), // (F)
    EMPTY_QUERY_RESPONSE_TYPE('I', 4, false, true), // (B)
    ERROR_RESPONSE_TYPE     ('E', 1024, false, true), // (B)
    EXECUTE_TYPE            ('E', 1024, true, false, ErrorMode.EXTENDED), // (F)
    FLUSH_TYPE              ('H', 4,   true, false), // (F)
    FUNCTION_CALL_TYPE      ('F', Integer.MAX_VALUE,  true, false), // (F)
    FUNCTION_CALL_RESPONSE_TYPE('V', -1,  false, true), // (B)
    NO_DATA_TYPE            ('n', 4, false, true), // (B)
    NOTICE_RESPONSE_TYPE    ('N', 1024, false, true), // (B)
    NOTIFICATION_RESPONSE_TYPE ('A', 1024, false, true), // (B)
    PARAMETER_DESCRIPTION_TYPE ('t', 1024, false, true), // (B)
    PARAMETER_STATUS_TYPE   ('S', 1024, false, true), // (B)
    PARSE_TYPE              ('P', Integer.MAX_VALUE, true, false, ErrorMode.EXTENDED), // (F)
    PARSE_COMPLETE_TYPE     ('1', 4, false, true), // (B)
    PASSWORD_MESSAGE_TYPE   ('p', 2048, true, false, ErrorMode.FATAL), // (F) - also GSS
    PORTAL_SUSPENDED_TYPE   ('s', 4, false, true), // (B)
    QUERY_TYPE              ('Q', Integer.MAX_VALUE, true, false, ErrorMode.SIMPLE), // (F)
    READY_FOR_QUERY_TYPE    ('Z', 5, false, true), // (B)
    ROW_DESCRIPTION_TYPE    ('T', Integer.MAX_VALUE, false, true), // (B)
    STARTUP_MESSAGE_TYPE    (0,   Integer.MAX_VALUE, true, false, ErrorMode.FATAL), // (F)
    SYNC_TYPE               ('S', 4, true, false), // (F)
    TERMINATE_TYPE          ('X', 4, true, false); // (F)
    
    // ErrorMode determines how server errors are recovered from when they occur. 
    // NONE - no recovery - connection is closed.
    // SIMPLE - error sent to user, server reset for next message.
    // EXTENDED - error sent to user, server waits for sync message. 
    // FATAL - error sent to user, server shuts down.
    public static enum ErrorMode { NONE, SIMPLE, EXTENDED, FATAL };
    
    private final int code;
    private final int size; 
    private final boolean readType;
    private final boolean writeType;
    private final ErrorMode errorMode;
    
    private final static Map<Integer, PostgresMessages> readMessages;
    private final static Map<Integer, PostgresMessages> writeMessages; 
    
    private PostgresMessages (int code, int size, boolean read, boolean write) {
        this (code, size, read, write, ErrorMode.NONE);
    }
    private PostgresMessages (int code, int size, boolean read, boolean write, ErrorMode error) {
        this.code = code;
        this.size = size;
        this.readType = read;
        this.writeType = write;
        this.errorMode = error;
    }
    
    public int code() { return this.code; }
    
    public int maxSize() { return this.size; }
    
    public boolean isReadMessage() { return this.readType; }
    
    public boolean isWriteMessage() { return this.writeType; }
    
    public ErrorMode errorMode() { return this.errorMode; }
    
    public static boolean readTypeCorrect (final int type) { return readMessages.containsKey(type); }
    public static boolean writeTypeCorrect (final int type) { return writeMessages.containsKey(type); }
    
    public static PostgresMessages messageType (final int typeCode) { return readMessages.get(typeCode); }

    public String toString()
    {
        char messageType =
            code == 0 ? '0' :
            code == -1 ? '-' : (char) code;
        return String.format("[%c]", messageType);
    }
    
    static {
        readMessages = new HashMap<>();
        writeMessages = new HashMap<>();
        for (PostgresMessages msg : PostgresMessages.values()) {
            if (msg.readType) {
                readMessages.put(msg.code, msg);
            }
            if (msg.writeType) {
                writeMessages.put(msg.code, msg);
            }
        }
    }
}
