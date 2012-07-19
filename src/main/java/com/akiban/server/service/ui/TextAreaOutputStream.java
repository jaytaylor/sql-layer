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

package com.akiban.server.service.ui;

import java.awt.*;
import javax.swing.*;

import java.io.IOException;
import java.io.OutputStream;

public class TextAreaOutputStream extends OutputStream
{
    private JTextArea textArea;
    private final StringBuilder buffer = new StringBuilder();
    private final OutputStream oldStream;
    private int writeOldFrom;
    
    // Initialize in deferred mode.
    public TextAreaOutputStream() {
        this.oldStream = System.out;
    }

    public TextAreaOutputStream(JTextArea textArea) {
        this.textArea = textArea;
        this.oldStream = null;
    }

    public synchronized void write(int b) {
        buffer.append((char)b);
    }

    public synchronized void write(byte[] b) {
        buffer.append(new String(b));
    }

    public synchronized void write(byte[] b, int off, int len) {
        buffer.append(new String(b, off, len));
    }

    public synchronized void flush() throws IOException {
        final JTextArea textArea = this.textArea;
        if (textArea != null) {
            final String string = buffer.toString();
            buffer.setLength(0);
            writeOldFrom = 0;
            SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        textArea.append(string);
                    }
                });
        }
        else if (oldStream != null) {
            // Keep buffering in hopes that the console will appear, but still output
            // to stdout just in case.
            int end = buffer.length();
            oldStream.write(buffer.subSequence(writeOldFrom, end).toString().getBytes());
            writeOldFrom = end;
        }
    }

    // Change stream to outputting to text area.
    public synchronized boolean setTextAreaIfUnbound(JTextArea textArea) {
        if ((this.textArea == null) && (oldStream != null)) {
            this.textArea = textArea;
            return true;
        }
        return false;
    }

    // Change stream back to original output for any messages after frame is closed.
    public synchronized boolean clearTextAreaIfBound(JTextArea textArea) {
        if (this.textArea == textArea) {
            this.textArea = null;
            return true;
        }
        return false;
    }

}
