package com.akiban.ais.ddl;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.*;
import static junit.framework.Assert.assertEquals;

public final class DDLViewerGUITest
{
    @Test(expected=NullPointerException.class)
    public void appendNull() throws Exception  {
        DDLViewerGUI viewer = new DDLViewerGUI();
        viewer.appendSqlText(null);
    }

    @Test
    public void appendOnlyWhitespace() throws IOException {
        DDLViewerGUI viewer = new DDLViewerGUI();
        assertFalse("append returend true", viewer.appendSqlText(new StringReader("     \t  ")));
    }

    @Test
    public void appendThrowsException() throws IOException {
        DDLViewerGUI viewer = new DDLViewerGUI();
        final String MESSAGE = "this is some fairly unique message";
        final String FIRST_TEXT = "the first bit of string I put in";

        assertTrue("append returned false", viewer.appendSqlText(new StringReader(FIRST_TEXT)));

        Reader reader = new Reader() {
            int successes = 3;
            @Override
            public int read(char[] cbuf, int off, int len) throws IOException {
                if (successes-- > 0) {
                    return len;
                }
                throw new IOException(MESSAGE);
            }

            @Override
            public void close() throws IOException {
            }
        };

        try {
            viewer.appendSqlText(reader);
        } catch (IOException e) {
            assertEquals("exception message", MESSAGE, e.getMessage());
        }

        assertEquals("sql text", FIRST_TEXT, viewer.getSqlText());
    }

    @Test
    public void goodGrouping() throws Exception {
        DDLViewerGUI viewer = new DDLViewerGUI();
        final String SQL =
                "CREATE TABLE customer (id int, PRIMARY KEY (id) ) engine = akibadb;\n" +
                "CREATE TABLE order(id int, cid int, PRIMARY KEY (id)," +
                "CONSTRAINT __akiban_fk0 FOREIGN KEY __akiban_fk0 (cid) REFERENCES customer(id) ) engine = akibadb;";
        final String GROUPING =
                "groupschema NONE\n" +
                "\n" +
                "CREATE GROUP\n" +
                "ROOT TABLE customer\n" +
                "(\n" +
                "    TABLE order (cid) REFERENCES customer (id)\n" +
                ");";

        assertTrue("append returned false", viewer.appendSqlText(new StringReader(SQL)));
        assertEquals("grouping", GROUPING, viewer.getGrouping().toString());
    }

    private static class DummyViewer extends DDLViewerGUI {
        private final boolean hasStdin;
        private final List<String> files = new ArrayList<String>();
        private final List<String> closedFiles = new ArrayList<String>();
        private boolean stdinClosed = false;
        private boolean windowShown;
        private boolean outputShown;
        private boolean stdinThrows = false;
        private String fileThrows = null;

        private DummyViewer(boolean hasStdin) {
            this.hasStdin = hasStdin;
        }

        @Override
        Reader readStdin() {
            return new Reader() {
                int successes = hasStdin ? 1 : 0;
                @Override
                public int read(char[] cbuf, int off, int len) throws IOException {
                    if (successes -- > 0) {
                        return (new StringReader("test").read(cbuf, off, len));
                    }
                    if (stdinThrows) {
                        throw new IOException("stdin throws");
                    }
                    return -1;
                }

                @Override
                public boolean ready() throws IOException {
                    return successes > 0;
                }

                @Override
                public void close() {
                    stdinClosed = true;
                }
            };
        }

        @Override
        Reader readFile(final String name) throws FileNotFoundException {
            files.add(name);

            if (fileThrows != null && fileThrows.equals(name)) {
                return new Reader() {
                    @Override
                    public int read(char[] cbuf, int off, int len) throws IOException {
                        throw new FileNotFoundException(name);
                    }

                    @Override
                    public void close() {
                        closedFiles.add(name);
                    }
                };
            }

            return new StringReader(name.toUpperCase()) {
                @Override
                public void close() {
                    closedFiles.add(name);
                }
            };
        }

        @Override
        void showWindow() {
            windowShown = true;
        }

        @Override
        void writeStdout() {
            outputShown = true;
        }

        public void assertClosed() {
            assertTrue("stdin not closed", stdinClosed);
            assertEquals("closed files", files, closedFiles);
        }
    }

    @Test
    public void noStdinOrArgs() throws IOException {
        DummyViewer viewer = new DummyViewer(false);
        viewer.start();

        assertEquals("window shown", true, viewer.windowShown);
        assertEquals("output shown", false, viewer.outputShown);
        assertEquals("files", Arrays.<String>asList(), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }
    
    @Test
    public void justStdin() throws IOException {
        DummyViewer viewer = new DummyViewer(true);
        viewer.start();

        assertEquals("window shown", false, viewer.windowShown);
        assertEquals("output shown", true, viewer.outputShown);
        assertEquals("files", Arrays.<String>asList(), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "-- stdin:\n\ntest";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }

    @Test
    public void justArgs() throws IOException {
        DummyViewer viewer = new DummyViewer(false);
        viewer.start("one", "two");

        assertEquals("window shown", false, viewer.windowShown);
        assertEquals("output shown", true, viewer.outputShown);
        assertEquals("files", Arrays.asList("one", "two"), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "-- file one:\n\nONE\n\n\n-- file two:\n\nTWO";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }

    @Test
    public void argsPlusGUI() throws IOException {
        DummyViewer viewer = new DummyViewer(false);
        viewer.start("one", "two", "--gui");

        assertEquals("window shown", true, viewer.windowShown);
        assertEquals("output shown", false, viewer.outputShown);
        assertEquals("files", Arrays.asList("one", "two"), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "-- file one:\n\nONE\n\n\n-- file two:\n\nTWO";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }

    @Test
    public void stdPlusArgs() throws IOException {
        DummyViewer viewer = new DummyViewer(true);
        viewer.start("one");

        assertEquals("window shown", false, viewer.windowShown);
        assertEquals("output shown", true, viewer.outputShown);
        assertEquals("files", Arrays.asList("one"), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "-- stdin:\n\ntest\n\n\n-- file one:\n\nONE";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }

    @Test
    public void argsOnlyGUI() throws IOException {
        DummyViewer viewer = new DummyViewer(false);
        viewer.start("--gui");

        assertEquals("window shown", true, viewer.windowShown);
        assertEquals("output shown", false, viewer.outputShown);
        assertEquals("files", Arrays.<String>asList(), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }

    @Test
    public void stdinThrowsException() throws IOException {
        DummyViewer viewer = new DummyViewer(true);
        viewer.stdinThrows = true;
        boolean thrown = false;
        try {
            viewer.start("one", "two", "three", "four");
        } catch (IOException e) {
            assertEquals("message", "stdin throws", e.getMessage());
            thrown = true;
        }
        assertTrue("no exception thrown", thrown);

        assertEquals("window shown", false, viewer.windowShown);
        assertEquals("output shown", false, viewer.outputShown);
        assertEquals("files", Arrays.<String>asList(), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "-- stdin:\n\n";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }

    @Test
    public void fileThrowsException() throws IOException {
        DummyViewer viewer = new DummyViewer(false);
        viewer.fileThrows = "three";
        boolean thrown = false;
        try {
            viewer.start("one", "two", "three", "four");
        } catch (FileNotFoundException e) {
            assertEquals("message", "three", e.getMessage());
            thrown = true;
        }
        assertTrue("no exception thrown", thrown);

        assertEquals("window shown", false, viewer.windowShown);
        assertEquals("output shown", false, viewer.outputShown);
        assertEquals("files", Arrays.asList("one", "two", "three"), viewer.files);
        viewer.assertClosed();

        final String EXPECT_SQL = "-- file one:\n\nONE\n\n\n-- file two:\n\nTWO\n\n\n-- file three:\n\n";
        assertEquals("sql text", EXPECT_SQL, viewer.getSqlText());
    }
}
