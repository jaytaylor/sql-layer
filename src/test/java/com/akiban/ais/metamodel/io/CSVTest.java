/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AkibanInformationSchema;
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
            ais1 = new SchemaDefToAis(SchemaDef.parseSchema(ddlString), false).getAis();
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
