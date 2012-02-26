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

package com.akiban.ais.ddl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.akiban.ais.model.AkibanInformationSchema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.akiban.ais.metamodel.io.MessageTarget;
import com.akiban.ais.metamodel.io.Writer;


/**
 * Entry point for reading a schema and producing an AIS in SQL or
 * binary format. All real work is done by SchemaDef and SchemaDefToAis.
 */
public class DDLSource {
    private final static int MAX_AIS_SIZE = 1 << 20; // 1MB, implied by net msg
    private final static String SQL_FORMAT = "sql";
    private final static String BINARY_FORMAT = "binary";

    
    public static void main(final String[] args) throws Exception {
        String format = SQL_FORMAT;
        InputStream inputStream = System.in;
        OutputStream outputStream = System.out;

        Options options = new Options();
        options.addOption("i", "input-file", true, "Input file name [stdin]");
        options.addOption("o", "output-file", true, "Output file name [stdout]");
        options.addOption("f", "format", true, "Output format, sql or binary [sql]");
        options.addOption("h", "help", false, "Print this message");
        
        try {
            CommandLineParser parser = new PosixParser();
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("DDLSource", options);
                return;
            }

            if (line.hasOption("input-file")) {
                String inputFile = line.getOptionValue("input-file");
                inputStream = new FileInputStream(inputFile);
            }

            if (line.hasOption("output-file")) {
                String oFileName = line.getOptionValue("output-file");
                outputStream = new FileOutputStream(oFileName, false);
            }

            if (line.hasOption("format")) {
                format = line.getOptionValue("format").toLowerCase();
                if (!format.equals(BINARY_FORMAT) && !format.equals(SQL_FORMAT)) {
                    System.err.println("Invalid format option: " + format);
                    System.exit(1);
                }
            }
        } catch (org.apache.commons.cli.ParseException exp) {
            System.err.println(exp.getMessage());
            System.exit(1);
        }

        final SchemaDef schemaDef = SchemaDef.parseSchemaFromStream(inputStream);
        final SchemaDefToAis toAis = new SchemaDefToAis(schemaDef, false);
        final AkibanInformationSchema ais = toAis.getAis();

        if (format.compareTo(SQL_FORMAT) == 0) {
            final PrintWriter pw = new PrintWriter(outputStream);
            final SqlTextTarget target = new SqlTextTarget(pw);
            final Writer writer = new Writer(target);
            writer.save(ais);
            target.writeGroupTableDDL(ais);
            pw.close();
        } else {
            final ByteBuffer rawAis = ByteBuffer.allocate(MAX_AIS_SIZE);
            rawAis.order(ByteOrder.LITTLE_ENDIAN);
            final MessageTarget target = new MessageTarget(rawAis);
            final Writer writer = new Writer(target);
            writer.save(ais);
            rawAis.flip();
            outputStream.write(rawAis.array(), rawAis.position(), rawAis.limit());
            outputStream.close();
        }
    }
}
