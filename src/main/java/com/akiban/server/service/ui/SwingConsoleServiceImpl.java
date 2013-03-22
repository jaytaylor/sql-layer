
package com.akiban.server.service.ui;

import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import java.io.PrintStream;

import com.google.inject.Inject;

public class SwingConsoleServiceImpl implements SwingConsoleService, Service {
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
