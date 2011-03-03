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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import com.akiban.ais.model.AkibanInformationSchema;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.ddl.SchemaDef.UserTableDef;
import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Writer;


/**
 * TODO - remove this class.  As of 1/5/2011 this class is no longer used by
 * server component. When studio and other components no longer need it, 
 * this class should be deleted.  Its logic has been divided into (a)
 * addition of parse capability in SchemaDef, and (b) new class
 * SchemaDefToAis.
 * 
 * This class reads the CREATE TABLE statements in a mysqldump file, plus
 * annotations to denote the group structure. There is neither an attempt to
 * fully parse the DDL, nor to handle syntactic variations. The purpose of this
 * class is to facilitate creating AIS instances from existing MySQL databases
 * prior to the arrival of the Control Center's implementation.
 * 
 * There is no error handling, and this is not a general-purpose parser. The
 * format of the text file must be exact, especially with respect to spaces,
 * back ticks, etc.
 * 
 * See the xxxxxxxx_schema.sql file in src/test/resources for an example of the
 * syntax.
 * 
 * @author peter
 * 
 */
public class DDLSource {

    public class ParseException extends Exception {
        private ParseException(Exception cause) {
            super(cause.getMessage(), cause);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(DDLSource.class.getName());

    private final static String SCHEMA_FILE_NAME = "src/test/resources/xxxxxxxx_schema.ddl";
    private final static int MAX_AIS_SIZE = 1048576;
    private final static String BINARY_FORMAT = "binary";
    private final static String SQL_FORMAT = "sql";

    private SchemaDef schemaDef;

    public static void main(final String[] args) throws Exception {

        String iFileName = SCHEMA_FILE_NAME;
        String oFileName = "/tmp/"
                + new File(iFileName).getName().split("\\.")[0] + ".out";
        String format = SQL_FORMAT;

        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("i", "input-file", true, "default input file = "
                + iFileName);
        options.addOption("o", "output-file", true, "default output file = "
                + oFileName);
        options.addOption("f", "format", true,
                "valid values are sql and binary; the default is sql");
        options.addOption("h", "help", false, "print this message");

        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("DDLSource", options);
                return;
            }

            if (line.hasOption("input-file")) {
                iFileName = line.getOptionValue("input-file");
            }

            if (line.hasOption("output-file")) {
                oFileName = line.getOptionValue("output-file");
            }

            if (line.hasOption("format")) {
                format = line.getOptionValue("format");
                format = format.toLowerCase();
                if (format.compareTo(BINARY_FORMAT) != 0
                        && format.compareTo(SQL_FORMAT) != 0) {
                    System.out.println("invald format option " + format
                            + "; using default = " + SQL_FORMAT);
                    format = SQL_FORMAT;
                }
            }
        } catch (org.apache.commons.cli.ParseException exp) {
            System.out.println("Unexpected exception:" + exp.getMessage());
        }

        final DDLSource source = new DDLSource();
        final AkibanInformationSchema ais = source.buildAISFromFile(iFileName);
        // AISPrinter.print(ais);

        if (format.compareTo(SQL_FORMAT) == 0) {
            final PrintWriter pw = new PrintWriter(new FileWriter(oFileName));
            SqlTextTarget target = new SqlTextTarget(pw);
            new Writer(target).save(ais);
            target.writeGroupTableDDL(ais);
            pw.close();
        } else {
            assert format.compareTo(BINARY_FORMAT) == 0;

            ByteBuffer rawAis = ByteBuffer.allocate(MAX_AIS_SIZE);
            rawAis.order(ByteOrder.LITTLE_ENDIAN);
            new Writer(new MessageTarget(rawAis)).save(ais);
            rawAis.flip();

            boolean append = false;
            File file = new File(oFileName);
            try {
                FileChannel wChannel = new FileOutputStream(file, append)
                        .getChannel();
                wChannel.write(rawAis);
                wChannel.close();
            } catch (IOException e) {
                throw new Exception("rarrrgh");
            }
        }
    }

    public DDLSource() {
    }

    public AkibanInformationSchema buildAISFromFile(final String fileName) throws Exception {
        this.schemaDef = SchemaDef.parseSchemaFromFile(fileName);
        return schemaDefToAis();
    }

    public AkibanInformationSchema buildAISFromString(final String schema) throws Exception {
        this.schemaDef = SchemaDef.parseSchema(schema);
        return schemaDefToAis();
    }

    public UserTableDef parseCreateTable(final String createTableStatement) throws Exception {
        this.schemaDef = new SchemaDef();
        try {
            return schemaDef.parseCreateTable(createTableStatement);
        } catch(RuntimeException e) {
            throw new ParseException(e);
        }
    }

    public AkibanInformationSchema buildAISFromBuilder(final String string) throws Exception {
        this.schemaDef = SchemaDef.parseSchema(string);
        return schemaDefToAis();
    }

    private AkibanInformationSchema schemaDefToAis() throws Exception {
        SchemaDefToAis toAis = new SchemaDefToAis(this.schemaDef, false);
        return toAis.getAis();
    }
}
