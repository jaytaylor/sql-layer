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

import com.akiban.server.error.InvalidParameterValueException;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Basic implementation of Postgres wire protocol for SQL integration.
 *
 * See http://developer.postgresql.org/pgdocs/postgres/protocol.html
 */
public class PostgresMessenger implements DataInput, DataOutput
{
    /*** Message Formats ***/
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

    private final static InOutTap waitTap = Tap.createTimer("sql: msg: wait");
    private final static InOutTap recvTap = Tap.createTimer("sql: msg: recv");
    private final static InOutTap xmitTap = Tap.createTimer("sql: msg: xmit");

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInput;
    private DataInputStream messageInput;
    private ByteArrayOutputStream byteOutput;
    private DataOutputStream messageOutput;
    private String encoding = "UTF-8";

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
        String newEncoding = encoding;
        if ((newEncoding == null) || newEncoding.equals("UNICODE"))
            newEncoding = "UTF-8";
        else if (newEncoding.startsWith("WIN") && newEncoding.matches("WIN\\d+"))
            newEncoding = "Cp" + newEncoding.substring(3);
        try {
            Charset.forName(newEncoding);
        }
        catch (IllegalArgumentException ex) {
            throw new InvalidParameterValueException("unknown client_encoding '" + 
                                                     encoding + "'");
        }
        this.encoding = newEncoding;
    }

    /** Read the next message from the stream, without any type opcode. */
    protected PostgresMessages readMessage() throws IOException {
        return readMessage(true);
    }
    /** Read the next message from the stream, starting with the message type opcode. */
    protected PostgresMessages readMessage(boolean hasType) throws IOException {
        PostgresMessages type;
        int code = -1;
        if (hasType) {
            try {
                waitTap.in();
                code = dataInput.read();
                if (!PostgresMessages.readTypeCorrect(code)) {
                    throw new IOException ("Bad protocol read message: " + (char)code);
                }
                type = PostgresMessages.messageType(code);
            }
            finally {
                waitTap.out();
            }
        }
        else {
            type = PostgresMessages.STARTUP_MESSAGE_TYPE;
            code = 0;
        }
            
        if (code < 0) 
            return PostgresMessages.EOF_TYPE;                            // EOF
        recvTap.in();
        try {
            int len = dataInput.readInt();
            if ((len < 0) || (len > type.maxSize()))
                throw new IOException(String.format("Implausible message length (%d) received.", len));
            len -= 4;
            try {
                byte[] msg = new byte[len];
                dataInput.readFully(msg, 0, len);
                messageInput = new DataInputStream(new ByteArrayInputStream(msg));
            } catch (OutOfMemoryError ex) {
                throw new IOException (String.format("Unable to allocate read buffer of length (%d)", len));
            }
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
        messageOutput.flush();
        byte[] msg = byteOutput.toByteArray();
        
        // check we're writing an allowed message. 
        assert PostgresMessages.writeTypeCorrect((int)msg[0]) : "Invalid write message: " + (char)msg[0];
        
        int len = msg.length - 1;
        msg[1] = (byte)(len >> 24);
        msg[2] = (byte)(len >> 16);
        msg[3] = (byte)(len >> 8);
        msg[4] = (byte)len;
        outputStream.write(msg);
    }

    /** Send outgoing message and optionally flush stream. */
    protected void sendMessage(boolean flush) throws IOException {
        sendMessage();
        if (flush)
            flush();
    }

    protected void flush() throws IOException {
        try {
            xmitTap.in();
            outputStream.flush();
        }
        finally {
            xmitTap.out();
        }
    }

    /** Save whatever portion of the current message there is so that
     * something asynchronous can be sent. */
    protected Object suspendMessage() throws IOException {
        messageOutput.flush();
        return byteOutput;
    }

    /** Restore the state from {@link #suspendMessage}. */
    protected void resumeMessage(Object state) throws IOException {
        byteOutput = (ByteArrayOutputStream)state;
        messageOutput = new DataOutputStream(byteOutput);
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

    /** Write the raw contents of the given byte stream's buffer. */
    public void writeByteStream(ByteArrayOutputStream s) throws IOException {
        s.writeTo(messageOutput);
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
