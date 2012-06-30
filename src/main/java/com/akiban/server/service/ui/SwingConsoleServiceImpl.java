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

import com.akiban.server.service.Service;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import java.io.PrintStream;

public class SwingConsoleServiceImpl implements SwingConsoleService, Service<SwingConsoleService> 
{
    private SwingConsole console;
    private final PrintStream origOut = System.out;
    private final PrintStream origErr = System.err;

    @Override
    public final void start() {
        final SwingConsole console = new SwingConsole();
        this.console = console;
        System.setOut(console.openPrintStream(true));
        show();
    }

    @Override
    public final void stop() {
        final JFrame console = this.console;
        this.console = null;
        System.setOut(origOut);
        System.setErr(origErr);
        if (console != null) {
            SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        console.dispose();
                    }
                });
        }
    }
    
    @Override
    public void crash() {
        stop();
    }

    @Override
    public SwingConsoleService cast() {
        return this;
    }

    @Override
    public Class<SwingConsoleService> castClass() {
        return SwingConsoleService.class;
    }

    @Override
    public void show() {
        setVisible(true);
    }

    @Override
    public void hide() {
        setVisible(true);
    }
    
    private void setVisible(final boolean visible) {
        final JFrame console = this.console;
        if (console != null) {
            SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        console.setVisible(visible);
                    }
                });
        }
        else
            throw new IllegalStateException("No frame to show / hide.");
    }

    @Override
    public PrintStream getPrintStream() {
        return console.getPrintStream();
    }

}
