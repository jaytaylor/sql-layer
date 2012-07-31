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

import com.akiban.server.service.ServiceManager;

import java.awt.*;
import java.awt.event.*;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.*;
import javax.swing.text.*;

import java.net.URL;
import java.io.PrintStream;

public class SwingConsole extends JFrame implements WindowListener 
{
    public static final String TITLE = "Akiban Server";
    public static final String ICON_PATH = "Akiban_Server_128x128.png";

    private final ServiceManager serviceManager;
    private JTextArea textArea;
    private PrintStream printStream;
    private final String RUN_PSQL_CMD;
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
            String osName = System.getProperty("os.name");
            boolean macOSX = "Mac OS X".equals(osName);
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
            JMenuItem runPsql = run.add("Run PSQL client");
            
            if (macOSX)
                RUN_PSQL_CMD = "osascript -e 'tell application \"Terminal\" to do script \"psql -h localhost -p 15432\"'";
            else if (osName.startsWith("Window"))
                RUN_PSQL_CMD = "cmd.exe /c start psql -h localhost -p15432 ";
            else // assuming unix-based system
                RUN_PSQL_CMD = "xterm -e psql -h localhost -p15432";

            runPsql.addActionListener(new ActionListener()
            {
                @Override
                public void actionPerformed(ActionEvent ae)
                {
                    try
                    {
                        Runtime.getRuntime().exec(RUN_PSQL_CMD);
                    }
                    catch (IOException ex)
                    {
                        JOptionPane.showMessageDialog(null,
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
                                                   "Do you really want to quit Akiban-Server?",
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

}
