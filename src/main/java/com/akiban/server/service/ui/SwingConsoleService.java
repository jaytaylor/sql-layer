
package com.akiban.server.service.ui;

import java.io.PrintStream;

public interface SwingConsoleService
{
    /** Show the console window. */
    public void show();
    /** Hide the console window. */
    public void hide();
    /** Return a stream that prints to the console window. */
    public PrintStream getPrintStream();
}
