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

package com.akiban.sql.pg;

import com.akiban.util.Tap;

import java.io.*;
import java.util.*;

/**
 * Basic implementation of Postgres wire protocol for SQL integration.
 *
 * See http://developer.postgresql.org/pgdocs/postgres/protocol.html
 */
public class PostgresMessenger implements DataInput, DataOutput
{
    /*** Message Formats ***/
    public static final int AUTHENTICATION_TYPE = 'R'; // (B)
    public static final int BACKEND_KEY_DATA_TYPE = 'K'; // (B)
    public static final int BIND_TYPE = 'B'; // (F)
    public static final int BIND_COMPLETE_TYPE = '2'; // (B)
    public static final int CLOSE_TYPE = 'C'; // (F)
    public static final int CLOSE_COMPLETE_TYPE = '3'; // (B)
    public static final int COMMAND_COMPLETE_TYPE = 'C'; // (B)
    public static final int COPY_DATA_TYPE = 'd'; // (F & B)
    public static final int COPY_DONE_TYPE = 'c'; // (F & B)
    public static final int COPY_FAIL_TYPE = 'f'; // (F)
    public static final int COPY_IN_RESPONSE_TYPE = 'G'; // (B)
    public static final int COPY_OUT_RESPONSE_TYPE = 'H'; // (B)
    public static final int COPY_BOTH_RESPONSE_TYPE = 'W'; // (B)
    public static final int DATA_ROW_TYPE = 'D'; // (B)
    public static final int DESCRIBE_TYPE = 'D'; // (F)
    public static final int EMPTY_QUERY_RESPONSE_TYPE = 'I'; // (B)
    public static final int ERROR_RESPONSE_TYPE = 'E'; // (B)
    public static final int EXECUTE_TYPE = 'E'; // (F)
    public static final int FLUSH_TYPE = 'H'; // (F)
    public static final int FUNCTION_CALL_TYPE = 'F'; // (F)
    public static final int FUNCTION_CALL_RESPONSE_TYPE = 'V'; // (B)
    public static final int NO_DATA_TYPE = 'n'; // (B)
    public static final int NOTICE_RESPONSE_TYPE = 'N'; // (B)
    public static final int NOTIFICATION_RESPONSE_TYPE = 'A'; // (B)
    public static final int PARAMETER_DESCRIPTION_TYPE = 't'; // (B)
    public static final int PARAMETER_STATUS_TYPE = 'S'; // (B)
    public static final int PARSE_TYPE = 'P'; // (F)
    public static final int PARSE_COMPLETE_TYPE = '1'; // (B)
    public static final int PASSWORD_MESSAGE_TYPE = 'p'; // (F)
    public static final int PORTAL_SUSPENDED_TYPE = 's'; // (B)
    public static final int QUERY_TYPE = 'Q'; // (F)
    public static final int READY_FOR_QUERY_TYPE = 'Z'; // (B)
    public static final int ROW_DESCRIPTION_TYPE = 'T'; // (B)
    public static final int STARTUP_MESSAGE_TYPE = 0; // (F)
    public static final int SYNC_TYPE = 'S'; // (F)
    public static final int TERMINATE_TYPE = 'X'; // (F)
    
    public static final int VERSION_CANCEL = 80877102; // 12345678
    public static final int VERSION_SSL = 80877103; // 12345679
    
    public static final int AUTHENTICATION_OK = 0;
    public static final int AUTHENTICATION_KERBEROS_V5 = 2;
    public static final int AUTHENTICATION_CLEAR_TEXT = 3;
    public static final int AUTHENTICATION_MD5 = 5;
    public static final int AUTHENTICATION_SCM = 6;
    public static final int AUTHENTICATION_GSS = 7;
    public static final int AUTHENTICATION_SSPI = 9;
    public static final int AUTHENTICATION_GSS_CONTINUE = 8;

    private final static Tap waitTap = Tap.add(new Tap.PerThread("sql: msg: wait", Tap.TimeAndCount.class));
    private final static Tap recvTap = Tap.add(new Tap.PerThread("sql: msg: recv", Tap.TimeAndCount.class));
    private final static Tap xmitTap = Tap.add(new Tap.PerThread("sql: msg: xmit", Tap.TimeAndCount.class));

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInput;
    private DataInputStream messageInput;
    private ByteArrayOutputStream byteOutput;
    private DataOutputStream messageOutput;
    private String encoding = "ISO-8859-1";
    private boolean cancel = false;

    public PostgresMessenger(InputStream inputStream, OutputStream outputStream) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.dataInput = new DataInputStream(inputStream);
    }

    InputStream getInputStream() {
        return inputStream;
    }

    OutputStream getOutputStream() {
        return outputStream;
    }

    /** The encoding used for strings. */
    public String getEncoding() {
        return encoding;
    }
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /** Has a cancel been sent? */
    public synchronized boolean isCancel() {
        return cancel;
    }
    /** Mark as cancelled. Cleared at the start of results. 
     * Usually set from a thread running a request just for that purpose. */
    public synchronized void setCancel(boolean cancel) {
        this.cancel = cancel;
    }

    /** Read the booleanNext message from the stream, without any type opcode. */
    protected int readMessage() throws IOException {
        return readMessage(true);
    }
    /** Read the booleanNext message from the stream, starting with the message type opcode. */
    protected int readMessage(boolean hasType) throws IOException {
        int type;
        if (hasType) {
            try {
                waitTap.in();
                type = dataInput.read();
            }
            finally {
                waitTap.out();
            }
        }
        else
            type = STARTUP_MESSAGE_TYPE;
        if (type < 0) 
            return type;                            // EOF
        try {
            recvTap.in();
            int len = dataInput.readInt();
            len -= 4;
            if ((len < 0) || (len > 0x8000))
                throw new IOException(String.format("Implausible message length (%d) received.", len));
            byte[] msg = new byte[len];
            dataInput.readFully(msg, 0, len);
            messageInput = new DataInputStream(new ByteArrayInputStream(msg));
            return type;
        }
        finally {
            recvTap.out();
        }
    }

    /** Begin outgoing message of given type. */
    protected void beginMessage(int type) throws IOException {
        byteOutput = new ByteArrayOutputStream();
        messageOutput = new DataOutputStream(byteOutput);
        messageOutput.write(type);
        messageOutput.writeInt(0);
    }

    /** Send outgoing message. */
    protected void sendMessage() throws IOException {
        sendMessage(false);
    }
    /** Send outgoing message and optionally flush stream. */
    protected void sendMessage(boolean flush) throws IOException {
        messageOutput.flush();
        byte[] msg = byteOutput.toByteArray();
        int len = msg.length - 1;
        msg[1] = (byte)(len >> 24);
        msg[2] = (byte)(len >> 16);
        msg[3] = (byte)(len >> 8);
        msg[4] = (byte)len;
        outputStream.write(msg);
        if (flush) {
            try {
                xmitTap.in();
                outputStream.flush();
            }
            finally {
                xmitTap.out();
            }
        }
    }

    /** Read null-terminated string. */
    public String readString() throws IOException {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        while (true) {
            int b = messageInput.read();
            if (b < 0) throw new IOException("EOF in the middle of a string");
            if (b == 0) break;
            bs.write(b);
        }
        return bs.toString(encoding);
    }

    /** Write null-terminated string. */
    public void writeString(String s) throws IOException {
        byte[] ba = s.getBytes(encoding);
        messageOutput.write(ba);
        messageOutput.write(0);
    }

    /*** DataInput ***/
    public boolean readBoolean() throws IOException {
        return messageInput.readBoolean();
    }
    public byte readByte() throws IOException {
        return messageInput.readByte();
    }
    public char readChar() throws IOException {
        return messageInput.readChar();
    }
    public double readDouble() throws IOException {
        return messageInput.readDouble();
    }
    public float readFloat() throws IOException {
        return messageInput.readFloat();
    }
    public void readFully(byte[] b) throws IOException {
        messageInput.readFully(b);
    }
    public void readFully(byte[] b, int off, int len) throws IOException {
        messageInput.readFully(b, off, len);
    }
    public int readInt() throws IOException {
        return messageInput.readInt();
    }
    @SuppressWarnings("deprecation")
    public String readLine() throws IOException {
        return messageInput.readLine();
    }
    public long readLong() throws IOException {
        return messageInput.readLong();
    }
    public short readShort() throws IOException {
        return messageInput.readShort();
    }
    public String readUTF() throws IOException {
        return messageInput.readUTF();
    }
    public int readUnsignedByte() throws IOException {
        return messageInput.readUnsignedByte();
    }
    public int readUnsignedShort() throws IOException {
        return messageInput.readUnsignedShort();
    }
    public int skipBytes(int n) throws IOException {
        return messageInput.skipBytes(n);
    }

    /*** DataOutput ***/
    public void write(byte[] data) throws IOException {
        messageOutput.write(data);
    }
    public void write(byte[] data, int ofs, int len) throws IOException {
        messageOutput.write(data, ofs, len);
    }
    public void write(int v) throws IOException {
        messageOutput.write(v);
    }
    public void writeBoolean(boolean v) throws IOException {
        messageOutput.writeBoolean(v);
    }
    public void writeByte(int v) throws IOException {
        messageOutput.writeByte(v);
    }
    public void writeBytes(String s) throws IOException {
        messageOutput.writeBytes(s);
    }
    public void writeChar(int v) throws IOException {
        messageOutput.writeChar(v);
    }
    public void writeChars(String s) throws IOException {
        messageOutput.writeChars(s);
    }
    public void writeDouble(double v) throws IOException {
        messageOutput.writeDouble(v);
    }
    public void writeFloat(float v) throws IOException {
        messageOutput.writeFloat(v);
    }
    public void writeInt(int v) throws IOException {
        messageOutput.writeInt(v);
    }
    public void writeLong(long v) throws IOException {
        messageOutput.writeLong(v);
    }
    public void writeShort(int v) throws IOException {
        messageOutput.writeShort(v);
    }
    public void writeUTF(String s) throws IOException {
        messageOutput.writeUTF(s);
    }

}
