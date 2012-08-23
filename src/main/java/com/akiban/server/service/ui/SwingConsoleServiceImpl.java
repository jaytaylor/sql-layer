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
import com.akiban.server.service.ServiceManager;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import java.io.PrintStream;

import com.google.inject.Inject;

public class SwingConsoleServiceImpl implements SwingConsoleService, Service<SwingConsoleService> 
{
    private final ServiceManager serviceManager;
    private final PrintStream origOut = System.out;
    private SwingConsole console;

    @Inject
    public SwingConsoleServiceImpl(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    @Override
    public final void start() {
        if (console == null) {
            console = new SwingConsole(serviceManager);
            PrintStream ps = console.openPrintStream(true);
            System.setOut(ps);
            ps.flush(); // Pick up output from before started.
        }
        show();
    }

    @Override
    public final void stop() {
        // If we stop while starting, that means some other service has problems.
        // Let the user see them before removing the console.
        switch (serviceManager.getState()) {
        case STARTING:
            return;
        }
        final JFrame console = this.console;
        this.console = null;
        System.setOut(origOut);
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
