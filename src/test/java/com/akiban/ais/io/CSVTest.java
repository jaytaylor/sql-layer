package com.akiban.ais.io;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.*;

public final class CSVTest {
    private static class TestBufferedReader extends BufferedReader {
        private final List<String> lines;
        private int truncatedLines = 0;
        private final int maxLines;

        private TestBufferedReader(java.io.Reader in, int maxLines) {
            super(in);
            this.maxLines = maxLines;
            lines = new ArrayList<String>(maxLines + 1);
        }

        @Override
        public String readLine() throws IOException {
            String line = super.readLine();
            lines.add(line);
            if (lines.size() > maxLines) {
                lines.remove(0);
                ++truncatedLines;
            }
            return line;
        }

        public int getTruncatedLines() {
            return truncatedLines;
        }

        public List<String> getLines() {
            return lines;
        }

        public String getLastLine() {
            return lines.get(lines.size()-1);
        }
    }

    private static String loadWriteRead(String file, boolean useReader) throws Exception {
        InputStream is = CSVTest.class.getResourceAsStream(file);
        assertNotNull("null IS: " + file, is);
        final String ddlString;
        try {
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader( new InputStreamReader(is) );
            for(String line; null != (line=br.readLine()); ) {
                sb.append(line);
            }
            ddlString = sb.toString();
        } finally {
            is.close();
        }

        final AkibaInformationSchema ais1;
        final String aisCSV;
        {
            DDLSource ddlSource = new DDLSource();
            ais1 = useReader ? ddlSource.buildAISFromBuilder(ddlString) : ddlSource.buildAISFromString(ddlString);
            StringWriter sWriter = new StringWriter();
            PrintWriter pWriter = new PrintWriter(sWriter);
            new Writer( (new CSVTarget(pWriter)) ).save(ais1);
            pWriter.flush();
            sWriter.flush();
            aisCSV = sWriter.toString();
        }

        TestBufferedReader aisReader = new TestBufferedReader( new StringReader(aisCSV), 5 );
        final AkibaInformationSchema ais2;
        try {
            ais2 = new Reader( new CSVSource(aisReader) ).load();
        } catch (Exception e) {
            System.err.println("At line " + (aisReader.getTruncatedLines() + aisReader.getLines().size()) );

            int lineNo = aisReader.getTruncatedLines();
            for(String line : aisReader.getLines()) {
                System.err.println(String.format("%-5d %s", ++lineNo, line));
            }
            throw e;
        }

        // validation goes here...
        assertEquals("u tables.size", ais1.getUserTables().size(), ais2.getUserTables().size());
        assertEquals("g tables.size", ais1.getGroupTables().size(), ais2.getGroupTables().size());

        return aisCSV;
    }

    @Test
    public void testTPCC1() throws Exception {
        loadWriteRead("tpcc.sql", true);
    }

    @Test
    public void testTPCC2() throws Exception {
        loadWriteRead("tpcc.sql", false);
    }
}
