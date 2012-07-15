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
import javax.swing.*;
import javax.swing.text.*;

import java.net.URL;
import java.io.OutputStream;
import java.io.PrintStream;

public class SwingConsole extends JFrame implements WindowListener 
{
    public static final String TITLE = "Akiban Server";
    public static final String ICON_PATH = "Akiban_Server_128x128.png";

    private final ServiceManager serviceManager;
    private JTextArea textArea;
    private PrintStream printStream;

    public SwingConsole(ServiceManager serviceManager) {
        super(TITLE);

        this.serviceManager = serviceManager;

        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        addWindowListener(this);
        {
            boolean macOSX = "Mac OS X".equals(System.getProperty("os.name"));
            int shift = (macOSX) ? InputEvent.META_MASK : InputEvent.CTRL_MASK;

            JMenuBar menuBar = new JMenuBar();

            if (!macOSX || !Boolean.getBoolean("apple.laf.useScreenMenuBar")) {
                JMenu fileMenu = new JMenu("File");
                fileMenu.setMnemonic(KeyEvent.VK_F);
                JMenuItem quitMenuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
                quitMenuItem.addActionListener(new ActionListener() {
                        public void actionPerformed(ActionEvent e) {
                            quit(true);
                        }
                    });
                quitMenuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, shift));
                fileMenu.add(quitMenuItem);
                menuBar.add(fileMenu);
            }
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
            menuBar.add(editMenu);

            setJMenuBar(menuBar);
        }
        textArea = new JTextArea(50, 100);
        textArea.setLineWrap(true);
        DefaultCaret caret = (DefaultCaret)textArea.getCaret();
        caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        add(scrollPane);

        pack();
        setLocation(200, 200);
        
        URL iconURL = SwingConsole.class.getClassLoader().getResource(SwingConsole.class.getPackage().getName().replace('.', '/') + "/" + ICON_PATH);
        if (iconURL != null) {
            ImageIcon icon = new ImageIcon(iconURL);
            setIconImage(icon.getImage());
        }
    }

    @Override
    public void windowClosing(WindowEvent arg0) {
        quit(false);
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

    protected void quit(boolean force) {
        switch (serviceManager.getState()) {
        default:
            try {
                serviceManager.stopServices();
            }
            catch (Exception ex) {
            }
            break;
        case IDLE:
        case ERROR_STARTING:
            if (force) {
                dispose();
            }
            break;
        }
    }

}
