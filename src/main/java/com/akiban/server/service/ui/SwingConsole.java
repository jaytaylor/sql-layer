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
import java.awt.event.*;
import javax.swing.*;
import javax.swing.text.*;

import java.io.OutputStream;

public class SwingConsole extends JFrame implements WindowListener 
{
    public static final String TITLE = "Akiban Server";

    private JTextArea textArea;

    public SwingConsole() {
        super(TITLE);
        
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        addWindowListener(this);
        {
            JMenuBar menuBar = new JMenuBar();

            JMenu fileMenu = new JMenu("File");
            fileMenu.setMnemonic(KeyEvent.VK_F);
            JMenuItem quitMenuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
            quitMenuItem.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        quit();
                    }
                });
            fileMenu.add(quitMenuItem);
            menuBar.add(fileMenu);

            JMenu editMenu = new JMenu("Edit");
            fileMenu.setMnemonic(KeyEvent.VK_E);
            Action action = new DefaultEditorKit.CutAction();
            action.putValue(Action.NAME, "Cut");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_X, InputEvent.CTRL_MASK));
            editMenu.add(action);
            action = new DefaultEditorKit.CopyAction();
            action.putValue(Action.NAME, "Copy");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_C, InputEvent.CTRL_MASK));
            editMenu.add(action);
            action = new DefaultEditorKit.PasteAction();
            action.putValue(Action.NAME, "Paste");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_V, InputEvent.CTRL_MASK));
            editMenu.add(action);    
            action = new TextAction(DefaultEditorKit.selectAllAction) {
                    public void actionPerformed(ActionEvent e) {
                        getFocusedComponent().selectAll();
                    }
                };
            action.putValue(Action.NAME, "Select All");
            action.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(KeyEvent.VK_A, InputEvent.CTRL_MASK));
            editMenu.add(action);    
            menuBar.add(editMenu);

            setJMenuBar(menuBar);
        }
        textArea = new JTextArea();
        DefaultCaret caret = (DefaultCaret)textArea.getCaret();
        caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
        textArea.setPreferredSize(new Dimension(500, 500));
        JScrollPane scrollPane = new JScrollPane(textArea, 
                                                 JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,
                                                 JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        textArea.setLineWrap(true);
        add(scrollPane);

        pack();
        setLocation(200, 200);
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
    
    public OutputStream textOutputStream() {
        return new TextAreaOutputStream(textArea);
    }

    protected void quit() {
        // TODO: There isn't any way for a service to rendezvous with
        // the service manager. Should there be?
        try {
            com.akiban.server.AkServer.procrunStop(new String[0]);
        }
        catch (Exception ex) {
        }
    }

}
