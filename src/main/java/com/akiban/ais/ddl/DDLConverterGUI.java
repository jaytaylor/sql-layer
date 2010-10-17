package com.akiban.ais.ddl;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

public final class DDLConverterGUI {

    private final JButton button = new JButton("Converter");
    private final JTextArea left = new JTextArea();
    private final JTextArea right = new JTextArea();

    public DDLConverterGUI() {
        JFrame window = new JFrame("DDL Converter");
        window.addWindowListener( new WindowAdapter(){
            @Override
            public void windowClosing(WindowEvent e) {
                System.exit(0);
            }
        });
        window.add( makeRootComponent() );
        window.setSize(700, 500);
        window.setVisible(true);
    }

    private JComponent makeRootComponent() {
        left.setEditable(true);
        right.setEditable(true);

        JPanel ret = new JPanel();
        ret.setLayout( new BorderLayout() );

        JSplitPane mainBox = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        mainBox.add(new JScrollPane(left));
        mainBox.add(new JScrollPane(right));

        ret.add(mainBox, BorderLayout.CENTER);
        ret.add(button, BorderLayout.SOUTH);

        button.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
                refresh();
            }
        });

        return ret;
    }

    private void refresh() {
        Reader in = new StringReader( left.getText() );
        String out;
        try {
            Writer outWriter = new StringWriter();
            DDLGroupingConverter.convert(in, outWriter);
            outWriter.flush();
            out = outWriter.toString();
            right.setForeground(Color.BLACK);
        }
        catch (Throwable e) {
            StringWriter err = new StringWriter();
            e.printStackTrace(new PrintWriter(err));
            err.flush();
            out = err.toString();
            right.setForeground(Color.RED);
        }
        right.setText(out);
    }

    public static void main(String[] ignored) {
        new DDLConverterGUI();
    }
}
