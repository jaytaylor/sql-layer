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

package com.foundationdb.sql.ui;

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
