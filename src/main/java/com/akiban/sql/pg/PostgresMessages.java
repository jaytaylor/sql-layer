/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.pg;

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
    PASSWORD_MESSAGE_TYPE   ('p', 2048, true, false), // (F) - also GSS
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
        readMessages = new HashMap<Integer,PostgresMessages>();
        writeMessages = new HashMap<Integer, PostgresMessages>();
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
