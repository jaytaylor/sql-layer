package com.akiban.ais.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import com.akiban.ais.io.CSVSource;
import com.akiban.ais.io.MessageSource;
import com.akiban.ais.io.MySQLSource;
import com.akiban.ais.io.Reader;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Source;

public class AISReader
{
    public static void main(String[] args) throws Exception
    {
        new AISReader(args).run();
    }

    private AISReader(String[] args)
    {
        int a = 0;
        String format = args[a++];
        try {
            if (format.equals("--mysql")) {
                String hostAndPort = args[a++];
                int colon = hostAndPort.indexOf(':');
                if (colon < 0) {
                    mysqlHost = hostAndPort;
                    mysqlPort = DEFAULT_MYSQL_PORT;
                } else {
                    mysqlHost = hostAndPort.substring(colon);
                    mysqlPort = Integer.parseInt(hostAndPort.substring(colon + 1));
                }
                while (a < args.length) {
                    String flag = args[a++];
                    String value = args[a++];
                    if (flag.equals("--user")) {
                        mysqlUser = value;
                    } else if (flag.equals("--password")) {
                        mysqlPassword = value;
                    } else {
                        usage();
                    }
                }
            } else if (format.equals("--csv")) {
                csvFilename = args[a++];
            } else if (format.equals("--java")) {
                javaFilename = args[a++];
            } else if (format.equals("--message")) {
                messageFilename = args[a++];
            } else {
                usage();
            }
        } catch (Exception e) {
            usage();
        }
    }

    private void run() throws Exception
    {
        AkibaInformationSchema ais = null;
        if (mysqlHost != null) {
            Source source = new MySQLSource(mysqlHost, Integer.toString(mysqlPort), mysqlUser, mysqlPassword);
            ais = new Reader(source).load();
        } else if (csvFilename != null) {
            Source source = new CSVSource(new BufferedReader(new FileReader(csvFilename)));
            ais = new Reader(source).load();
        } else if (javaFilename != null) {
            ais = (AkibaInformationSchema) new ObjectInputStream(new FileInputStream(javaFilename)).readObject();
        } else if (messageFilename != null) {
            FileInputStream fileInput = new FileInputStream(messageFilename);
            FileChannel fileChannel = fileInput.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate((int) fileChannel.size());
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            fileChannel.read(buffer);
            buffer.flip();
            fileInput.close(); // closes the channel too
            ais = new Reader(new MessageSource(buffer)).load();
        } else {
            usage();
        }
        assert ais != null;
        ObjectOutputStream output = new ObjectOutputStream(System.out);
        output.writeObject(ais);
    }

    private void usage()
    {
        for (String line : USAGE) {
            System.out.println(line);
        }
        System.exit(1);
    }

    private static final String[] USAGE = {
        "aisread --mysql HOST[:PORT] --user USER [--password PASSWORD]",
        "aisread --csv FILENAME",
        "aisread --java FILENAME",
        "",
        "--mysql reads an AIS from the database on the indicated HOST. If PORT is not specified, the MySQL default of ",
        "3306 is used. The database connection is made as USER, identified by PASSWORD if supplied.",
        "",
        "--csv reads a CSV-formatted AIS from FILENAME.",
        "",
        "--java reads a java-serialized AIS from FILENAME.",
        "",
        "--message reads a message-serialized AIS from FILENAME."
    };

    private static final int DEFAULT_MYSQL_PORT = 3306;

    private String mysqlHost;
    private int mysqlPort;
    private String mysqlUser;
    private String mysqlPassword;
    private String csvFilename;
    private String javaFilename;
    private String messageFilename;
}
