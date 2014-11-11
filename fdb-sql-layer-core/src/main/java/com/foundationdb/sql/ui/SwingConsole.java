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

import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.monitor.ServerMonitor;

import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.io.IOException;
import javax.swing.*;
import javax.swing.text.*;

import java.net.URL;
import java.io.PrintStream;

public class SwingConsole extends JFrame implements WindowListener 
{
    public static final String TITLE = "FoundationDB SQL Layer";
    public static final String ICON_PATH = "fdb_128x128.png";

    private final ServiceManager serviceManager;
    private JTextArea textArea;
    private PrintStream printStream;
    private  String[] RUN_FDBSQLCLI_CMD;
    private boolean adjusted = false;

    public SwingConsole(ServiceManager serviceManager) {
        super(TITLE);

        this.serviceManager = serviceManager;

        setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        
        // output area
        textArea = new JTextArea(50, 100);
        textArea.setLineWrap(true);
        DefaultCaret caret = (DefaultCaret)textArea.getCaret();
        caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        add(scrollPane);
        
        // menu
        addWindowListener(this);
        {
            final String osName = System.getProperty("os.name");
            final boolean macOSX = "Mac OS X".equals(osName);
            int shift = (macOSX) ? InputEvent.META_MASK : InputEvent.CTRL_MASK;

            JMenuBar menuBar = new JMenuBar();

            // File menu
            if (!macOSX || !Boolean.getBoolean("apple.laf.useScreenMenuBar")) {
                JMenu fileMenu = new JMenu("File");
                fileMenu.setMnemonic(KeyEvent.VK_F);
                JMenuItem quitMenuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
                quitMenuItem.addActionListener(new ActionListener() {
                        public void actionPerformed(ActionEvent e) {
                            quit();
                        }
                    });
                quitMenuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, shift));
                fileMenu.add(quitMenuItem);
                menuBar.add(fileMenu);
            }
            
            // Edit menu
            JMenu editMenu = new JMenu("Edit");
            editMenu.setMnemonic(KeyEvent.VK_E);
            Action action = new DefaultEditorKit.CutAction();
            action.putValue(Action.NAME, "Cut");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_X, shift));
            editMenu.add(action);
            action = new DefaultEditorKit.CopyAction();
            action.putValue(Action.NAME, "Copy");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_C, shift));
            editMenu.add(action);
            action = new DefaultEditorKit.PasteAction();
            action.putValue(Action.NAME, "Paste");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_V, shift));
            editMenu.add(action);    
            action = new TextAction(DefaultEditorKit.selectAllAction) {
                    public void actionPerformed(ActionEvent e) {
                        getFocusedComponent().selectAll();
                    }
                };
            action.putValue(Action.NAME, "Select All");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_A, shift));
            editMenu.add(action);
            
            JMenuItem clearAll = editMenu.add("Clear Console");
            clearAll.setMnemonic(KeyEvent.VK_R);
            clearAll.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_K, 
                                                           shift));
            clearAll.addActionListener(new ActionListener()
            {
                @Override
                public void actionPerformed(ActionEvent ae)
                {
                    textArea.setText("");
                }
                
            });
            
            menuBar.add(editMenu);
               
            // Run menu
            JMenu run = new JMenu("Run");
            run.setMnemonic(KeyEvent.VK_W);
            JMenuItem runPsql = run.add("Run FDB SQL client");
            runPsql.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F6,
                                                          shift));

            runPsql.addActionListener(new ActionListener()
            {
                @Override
                public void actionPerformed(ActionEvent ae)
                {
                   
                    if (!adjusted)
                    {
                        int port = getPostgresPort();
                        String host = getPostgresHost();

                        if (macOSX)
                            RUN_FDBSQLCLI_CMD = new String[]
                            {
                                "osascript",
                                "-e",
                                "tell application \"Terminal\"\n activate\n do script \"exec fdbsqlcli -h " + host + " -p " + port + "\"\n end tell"
                            };
                        else if (osName.startsWith("Window"))
                            RUN_FDBSQLCLI_CMD = new String[]
                            {
                                "cmd.exe",
                                "/c",
                                "start fdbsqlcli -h " + host + " -p " + port
                            };
                        else // assuming unix-based system
                            RUN_FDBSQLCLI_CMD = new String[]
                            {
                                new File("/etc/alternatives/x-terminal-emulator").exists()
                                    ? "x-terminal-emulator"
                                    : "xterm",
                                "-e",
                                "fdbsqlcli -h " + host + " -p " + port
                            };

                        adjusted = true;
                    }
                        
                    try
                    {
                        Runtime.getRuntime().exec(RUN_FDBSQLCLI_CMD);
                    }
                    catch (IOException ex)
                    {
                        JOptionPane.showMessageDialog(SwingConsole.this,
                                                      "Unable to open Terminal\nError: " + ex.getMessage(),
                                                      "Error",
                                                      JOptionPane.ERROR_MESSAGE);
                    }
                }
            });
            menuBar.add(run);
            
            
            setJMenuBar(menuBar);
        }

        // centerise the window
        pack();       
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        setSize(screenSize.width/2, screenSize.height/2);
        setLocationRelativeTo(null);
 
        
        URL iconURL = SwingConsole.class.getClassLoader().getResource(SwingConsole.class.getPackage().getName().replace('.', '/') + "/" + ICON_PATH);
        if (iconURL != null) {
            ImageIcon icon = new ImageIcon(iconURL);
            setIconImage(icon.getImage());
        }
    }

    
    @Override
    public void windowClosing(WindowEvent arg0) {
        quit();
    }
    
    @Override
    public void windowClosed(WindowEvent arg0) {
    }
    @Override
    public void windowActivated(WindowEvent arg0) {
    }
    @Override
    public void windowDeactivated(WindowEvent arg0) {
    }
    @Override
    public void windowDeiconified(WindowEvent arg0) {
    }
    @Override
    public void windowIconified(WindowEvent arg0) {
    }
    @Override
    public void windowOpened(WindowEvent arg0) {
    }            
    
    public PrintStream getPrintStream() {
        return printStream;
    }

    static class TextAreaPrintStream extends PrintStream {
        public TextAreaPrintStream() {
            this(new TextAreaOutputStream());
        }
        
        public TextAreaPrintStream(TextAreaOutputStream out) {
            super(out, true);
        }
        
        public TextAreaOutputStream getOut() {
            return (TextAreaOutputStream)out;
        }
    }

    public PrintStream openPrintStream(boolean reuseSystem) {
        if (reuseSystem &&
            (System.out instanceof TextAreaPrintStream) &&
            ((TextAreaPrintStream)System.out).getOut().setTextAreaIfUnbound(textArea)) {
            printStream = System.out;
        }
        else {
            printStream = new PrintStream(new TextAreaOutputStream(textArea));
        }
        return printStream;
    }

    public void closePrintStream() {
        if (printStream == System.out) {
            ((TextAreaPrintStream)System.out).getOut().clearTextAreaIfBound(textArea);
        }
        printStream = null;
    }

    protected void quit() {
        switch (serviceManager.getState()) {    
        case ERROR_STARTING:
            dispose();
            break;
        default:
            int yn = JOptionPane.showConfirmDialog(this, 
                                                   "Do you really want to quit?",
                                                   "Attention!", 
                                                    JOptionPane.YES_NO_OPTION,
                                                    JOptionPane.QUESTION_MESSAGE);

            if (yn != JOptionPane.YES_OPTION)
                return;
            try {
                serviceManager.stopServices();
            }
            catch (Exception ex) {
            }
            break;
        }
    }

    // Note that this needs to work even if services didn't start properly.
    protected int getPostgresPort() {
        MonitorService service = serviceManager.getMonitorService();
        if (service != null) {
            ServerMonitor monitor = service.getServerMonitors().get("Postgres");
            if (monitor != null) {
                int port = monitor.getLocalPort();
                if (port > 0)
                    return port;
            }
        }
        return 15432;
    }

    // Note that this needs to work even if services didn't start properly.
    protected String getPostgresHost() {
        MonitorService service = serviceManager.getMonitorService();
        if (service != null) {
            ServerMonitor monitor = service.getServerMonitors().get("Postgres");
            if (monitor != null) {
                String host = monitor.getLocalHost();
                if (host != null)
                    return host;
            }
        }
        return "localhost";
    }

}
