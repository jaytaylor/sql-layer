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

package com.akiban.ais.metamodel.io;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.rowdata.SchemaFactory;
import org.junit.Test;


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

    private static String loadWriteRead(String file) throws Exception {
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

        final AkibanInformationSchema ais1;
        final String aisCSV;
        {
            SchemaFactory schemaFactory = new SchemaFactory();
            ais1 = schemaFactory.ais(ddlString);
            StringWriter sWriter = new StringWriter();
            PrintWriter pWriter = new PrintWriter(sWriter);
            new Writer( (new CSVTarget(pWriter)) ).save(ais1);
            pWriter.flush();
            sWriter.flush();
            aisCSV = sWriter.toString();
        }

        TestBufferedReader aisReader = new TestBufferedReader( new StringReader(aisCSV), 5 );
        final AkibanInformationSchema ais2;
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
        loadWriteRead("tpcc.sql");
    }
}
