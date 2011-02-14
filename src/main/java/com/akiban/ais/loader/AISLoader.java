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

/*
 * AISLoader - this application will take in the following:
 *      -properties file listing the network configuration (addressing)
 *      -database user and password for each head
 *      -DDL file
 *      -Akiban Information Schema
 */
package com.akiban.ais.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.ddl.SqlTextTarget;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AkibanInformationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AISLoader - performs a simple load of the AIS to all the heads that
 * are specified in the system. Note that this is single threaded and is
 * not very robust with the error checking
 * 
 */
public class AISLoader
{
    private static final Logger logger = LoggerFactory.getLogger(AISLoader.class.getName());

    public static void main(final String[] args) throws Exception
    {
        new AISLoader(args).run();
    }

    private AISLoader(String[] args)
    {
        readArgs(args);

        InputStream propFile;
        Properties tempProp = new Properties();
        try {
            propFile = new FileInputStream(propertiesFileName);
            tempProp.load(propFile);
            propFile.close();
        }
        catch (IOException ioe) {
            logger.error("IO exception on properties file load: " + ioe.getMessage());
        }

        // parse the config file
        addressHeadIP = fetchArrayFromPropertiesFile("aisload.head.ip", tempProp);
        addressDBIP = fetchArrayFromPropertiesFile("aisload.db.host", tempProp);
        addressHeadPort = fetchArrayFromPropertiesFile("aisload.head.port", tempProp);
        addressDBPort = fetchArrayFromPropertiesFile("aisload.db.port", tempProp);
        users = fetchArrayFromPropertiesFile("aisload.db.user", tempProp);
        passwords = fetchArrayFromPropertiesFile("aisload.db.password", tempProp);
        databases = fetchArrayFromPropertiesFile("aisload.db.database", tempProp);
        sqlFilesToRun = fetchArrayFromPropertiesFile("aisload.db.sql.files", tempProp);

        // verify that we have a good load. This entails making sure everything matches up,
        // exists, and works
        if ((addressDBPort == null) ||
            (addressDBIP == null))
        {
            // this is an error
            logger.error("Address incomplete: " + addressDBIP +":" + addressDBPort);
        }
    }

    // gets a single array
    private String[] fetchArrayFromPropertiesFile(String propertyName, Properties propFile)
    {
        String temp = propFile.getProperty(propertyName);
        String[] array = null;

        try
        {
            if (temp != null)
            {
                array = temp.split(";");
            }
        } catch (NullPointerException e)
        {
            logger.error("Null pointer exception on fetch: " + e.getMessage());
        }

        return array;
    }

    // gets arrays within an array - may get used later for grouping like
    // properties like IP,Port
    private String[][] fetchStructArrayFromPropertiesFile(String propertyName, Properties propFile)
    {
        String temp = propFile.getProperty(propertyName);
        if (temp == null)
        {
            return null;
        }

        try
        {
            String[] tempArray = temp.split(";");
            String[][] array = new String[tempArray.length][tempArray.length];
            for (int ii=0; ii < tempArray.length; ii++) {
                array[ii] = tempArray[ii].split(",");
            }
            return array;
        } catch (NullPointerException e)
        {
            logger.error("Null pointer exception on fetch: " + e.getMessage());
            return null;
        }
    }

    private void readArgs(String[] args)
    {
        int a = 0;
        try {
            propertiesFileName = new File(args[a++]);
            groupingFileName = new File(args[a++]);
            schemaFileName = new File(args[a++]);
            aisSchemaFileName = new File(args[a++]);
        } catch (Exception e) {
            usage();
        }
    }

    private void run ()
    {
        String groupingDefinition = null;
        String tableStatements = null;

        // generate AIS
        logger.error("Setting AIS schema file to: " + aisSchemaFileName.getAbsolutePath());
        try
        {
            groupingDefinition = this.loadTextFile(groupingFileName.getPath());
            tableStatements = this.loadTextFile(schemaFileName.getPath());
            String aisData = this.generateAisInformation(groupingDefinition, tableStatements);

            // For all heads in properties file
            for (int ii=0; ii<addressDBIP.length; ii++) {
                String db, pass;
                if (databases.length <= ii ) {
                    db = databases[0];
                } else {
                    db = databases[ii];
                }
                if (passwords.length <= ii ) {
                    pass = passwords[0];
                } else {
                    pass = passwords[ii];
                }

                // Load AIS to head
                Connection connection = ConnectionUtils.connect(addressDBIP[ii],
                                                                addressDBPort[ii],
                                                                db,
                                                                users[ii],
                                                                pass);
                this.reloadAIS(aisData, connection);
                if (sqlFilesToRun == null) {
                    break;
                }
                for (int jj=0;jj<sqlFilesToRun.length;jj++) {
                    File sqlFileName = new File(sqlFilesToRun[jj]);
                    if (sqlFileName.exists() == true) {
                        String sqlText = this.loadTextFile(sqlFileName.getPath());
                        runSQLSuite(sqlText, connection);
                    }
                }
                ConnectionUtils.disconnect(connection);
            }
        }
        catch (Exception e)  {
            logger.error ("Failed to generate the AIS:" + e.getMessage());
            logger.error ("groupingDefinition:");
            logger.error (groupingDefinition);
            logger.error ("tableStatements:");
            logger.error (tableStatements);
            throw new RuntimeException("Failed to complete setup", e);
        }
    }

    // for now, I don't want to add a cross dependency to the system-test dir
    public String generateAisInformation(String groupingDefinition, String tableStatements)
    {
        try        {
            final String aisDDL = groupingDefinition + tableStatements;

            final DDLSource source = new DDLSource();
            final AkibanInformationSchema ais = source.buildAISFromString(aisDDL);
            final StringWriter buffer = new StringWriter();
            final PrintWriter pw = new PrintWriter(buffer);
            SqlTextTarget target = new SqlTextTarget(pw);
            new Writer(target).save(ais);
            target.writeGroupTableDDL(ais);
            return buffer.toString();
        } catch (Exception e)  {
            logger.error ("Failed to generate the AIS:" + e.getMessage());
            logger.error ("groupingDefinition:");
            logger.error (groupingDefinition);
            logger.error ("tableStatements:");
            logger.error (tableStatements);
            throw new RuntimeException("Failed to generate AIS", e);
        }
    }

    public void reloadAIS(String aisData, Connection connection)
    {
        String aisSchema = null;
        try
        {
            aisSchema = this.loadTextFile(aisSchemaFileName.getPath());
            ConnectionUtils.executeMultiQuery(connection, aisSchema);
            ConnectionUtils.executeMultiQuery(connection, aisData);
        }
        catch (Exception e)
        {
            logger.error("Failed to reload the AIS: " + e.getMessage());
            logger.error("aisSchema: " + aisSchema);
            logger.error("aisData: " + aisData);
            throw new RuntimeException(e);
        }
    }

    public String loadTextFile(String fileName) throws IOException
    {
        StringBuffer buffer = new StringBuffer();
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line;
        while( (line = reader.readLine()) != null)
        {
            buffer.append(line).append("\n");
        }
        return buffer.toString();
    }

    private void runSQLSuite(String sqlText, Connection connection) throws IOException
    {
        List<String> sqlQueries = parseStatements(new StringReader(sqlText));
        int index = 0;
        int failed = 0;
        for (String query : sqlQueries) {
            index++;
            Exception error = null;
            try {
                ConnectionUtils.execute(connection, query);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private List<String> parseStatements(Reader reader) throws IOException
    {
        List<String> statements = new ArrayList<String>(300);

        StringBuffer sql = new StringBuffer();
        String line;

        BufferedReader in = new BufferedReader(reader);

        while ((line = in.readLine()) != null)
        {
            if (!keepformat)
            {
                line = line.trim();
            }
            if (!keepformat)
            {
                if (line.startsWith("//"))
                {
                    continue;
                }
                if (line.startsWith("--"))
                {
                    continue;
                }
                StringTokenizer st = new StringTokenizer(line);
                if (st.hasMoreTokens())
                {
                    String token = st.nextToken();
                    if ("REM".equalsIgnoreCase(token))
                    {
                        continue;
                    }
                }
            }

            sql.append(keepformat ? "\n" : " ").append(line);

            // SQL defines "--" as a comment to EOL
            // and in Oracle it may contain a hint
            // so we cannot just remove it, instead we must end it
            if (!keepformat && line.indexOf("--") >= 0)
            {
                sql.append("\n");
            }
            if ((delimiterType.equals("normal") && endsWith(sql, delimiter))
                    || (delimiterType.equals("row") && line.equals(delimiter)))
            {
                String query = sql.substring(0, sql.length() - delimiter.length());
                if ("".equals(query.trim()) == false)
                {
                    statements.add(query);
                }
                sql.replace(0, sql.length(), "");
            }
        }
        // Catch any statements not followed by ;
        if (sql.length() > 0)
        {
            String query = sql.toString();
            if ("".equals(query.trim()) == false)
            {
                statements.add(query);
            }
        }
        return statements;
    }

    public static boolean endsWith(StringBuffer buffer, String suffix)
    {
        if (suffix.length() > buffer.length())
        {
            return false;
        }
        // this loop is done on purpose to avoid memory allocation performance
        // problems on various JDKs
        // StringBuffer.lastIndexOf() was introduced in jdk 1.4 and
        // implementation is ok though does allocation/copying
        // StringBuffer.toString().endsWith() does massive memory
        // allocation/copying on JDK 1.5
        // See http://issues.apache.org/bugzilla/show_bug.cgi?id=37169
        int endIndex = suffix.length() - 1;
        int bufferIndex = buffer.length() - 1;
        while (endIndex >= 0)
        {
            if (buffer.charAt(bufferIndex) != suffix.charAt(endIndex))
            {
                return false;
            }
            bufferIndex--;
            endIndex--;
        }
        return true;
    }

    private void usage()
    {
        for (String line : usage) {
            System.err.println(line);
        }
        System.exit(1);
    }

    private static final String scriptName = "AISLoader";
    private static final String[] usage = {
        String.format("%s PROPERTIES_FILE GROUP_FILE SCHEMA_FILE AIS SCHEMA FILE", scriptName),
        "    PROPERTIES_FILE: Cluster connection information",
        "    GROUP_FILE: Group file",
        "    SCHEMA_FILE: Schema file",
        "    AIS_SCHEMA_FILE: Generic AIS schema file",
    };

    // configuration information - all these are used to create and distribute the
    // AIS. The files are all passed in via the command line
    private File propertiesFileName;
    private File groupingFileName;
    private File schemaFileName;
    private File aisSchemaFileName;

    // the parameters below are all set via the properties file
    private String[] addressHeadIP;
    private String[] addressDBIP;
    private String[] addressHeadPort;
    private String[] addressDBPort;
    private String[] users;
    private String[] passwords;
    private String[] databases;
    private String[] sqlFilesToRun;

    // the folowing were all cut over along with the runSQL and lower code
    private Statement statement;
    private String onError = "continue";
    private boolean keepformat = false;
    private String delimiterType = "normal";
    private String delimiter = ";";
    private int totalSql = 0;
    private int goodSql = 0;
}
