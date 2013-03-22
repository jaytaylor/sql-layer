
package com.akiban.server.service.ui;

import com.akiban.server.AkServer;

import java.io.PrintStream;

public class AkServerWithSwingConsole
{
    public static void main(String[] args) {
        // This has to be done before log4j gets a chance to capture the previous
        // System.out for the CONSOLE appender. It will get switched to the real
        // console when that service starts up.
        PrintStream ps = new SwingConsole.TextAreaPrintStream();
        System.setOut(ps);
        try {
            AkServer.main(args);
        }
        catch (Exception ex) {
            ex.printStackTrace(ps);
        }
    }
}
