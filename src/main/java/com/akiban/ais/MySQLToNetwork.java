package com.akiban.ais;

import com.akiban.ais.io.MySQLSource;
import com.akiban.ais.io.Reader;
import com.akiban.ais.message.AISResponse;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.message.AkibanConnection;
import com.akiban.message.NettyAkibanConnectionImpl;
import com.akiban.message.MessageRegistry;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;

public class MySQLToNetwork
{
    public static void main(String[] args) throws Exception
    {
        new MySQLToNetwork(args).run();
    }

    private MySQLToNetwork(String[] args)
    {
        readArgs(args);
    }

    private void readArgs(String[] args)
    {
        int a = 0;
        try {
            dbHost = args[a++];
            dbPort = args[a++];
            dbUsername = args[a++];
            dbPassword = args[a++];
            netHost = args[a++];
            netPort = args[a];
        } catch (Exception e) {
            usage();
        }
    }

    private void run() throws Exception
    {
        MessageRegistry.initialize().registerModule("com.akiban.ais");
        sendAISToNetwork(readAISFromMySQL());
    }

    private AkibaInformationSchema readAISFromMySQL() throws Exception
    {
        return new Reader(new MySQLSource(dbHost, dbPort, dbUsername, dbPassword)).load();
    }

    private void sendAISToNetwork(AkibaInformationSchema ais) throws Exception
    {
        NetworkHandlerFactory.initializeNetwork("localhost", "9999",
                                                new CommEventNotifier()
                                                {
                                                    @Override
                                                    public void onConnect(AkibaNetworkHandler newHandler)
                                                    {
                                                    }
                                                    @Override
                                                    public void onDisconnect(AkibaNetworkHandler handler)
                                                    {
                                                    }
                                                });
        // NetworkHandlerFactory.initializeNetworkClient(null);
        AkibanConnection connection =
            NettyAkibanConnectionImpl.createConnection(NetworkHandlerFactory.getHandler(netHost, netPort, null));
        AISResponse aisResponse = new AISResponse(ais);
        connection.send(aisResponse);
    }

    private void usage()
    {
        for (String line : usage) {
            System.err.println(line);
        }
        System.exit(1);
    }

    private static final String scriptName = "getais";
    private static final String[] usage = {
        String.format("%s DB_HOST DB_USERNAME DB_PASSWORD DB_NAME NET_HOST NET_PORT", scriptName),
        "    DB_HOST: Host running database containing AIS",
        "    DB_USERNAME: Database username",
        "    DB_PASSWORD: Database password",
        "    NET_HOST: Host to receive AIS",
        "    NET_POST: Port to receive AIS"
    };

    private String dbHost;
    private String dbPort;
    private String dbUsername;
    private String dbPassword;
    private String netHost;
    private String netPort;
}
