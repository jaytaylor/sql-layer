/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.message;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.akiban.ais.BaseTestCase;
import com.akiban.ais.io.MySQLSource;
import com.akiban.ais.io.Reader;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Source;
import com.akiban.message.AkibanConnection;
import com.akiban.message.NettyAkibanConnectionImpl;
import com.akiban.message.MessageRegistry;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;

public class AISMessageTest extends BaseTestCase
{
    static
    {
        Logger rootLogger = Logger.getLogger("");
        Handler[] handlers = rootLogger.getHandlers();
        for (Handler handler : handlers) {
            handler.setLevel(Level.WARNING);
        }
    }

    public void test() throws Exception
    {
        if(isDatabaseAvailable() == false) {
            return;
        }
        
        MessageRegistry.initialize().registerModule("com.akiban.ais");
        AkibaInformationSchema ais = readAISFromMySQL();
        startServer(ais);
        AkibaInformationSchema ais2 = runClient();
        checkAIS(ais, ais2);
    }

    private ByteBuffer serialize(AkibaInformationSchema ais) throws Exception
    {
        ByteBuffer payload = ByteBuffer.wrap(new byte[1000000]);
        AISResponse aisResponse = new AISResponse(ais);
        aisResponse.write(payload);
        payload.flip();
        return payload;
    }

    private void checkAIS(AkibaInformationSchema ais1, AkibaInformationSchema ais2) throws Exception
    {
        assertEquals(serialize(ais1), serialize(ais2));
    }

    private void startServer(AkibaInformationSchema ais)
    {
        ServerEventHandler serverEventHandler = new ServerEventHandler(ais);
        NetworkHandlerFactory.initializeNetwork("localhost", "8080", serverEventHandler);
    }

    private AkibaInformationSchema runClient() throws Exception
    {
        //NetworkHandlerFactory.initializeNetworkClient(null);
        AkibanConnection connection =
            NettyAkibanConnectionImpl.createConnection(NetworkHandlerFactory.getHandler("localhost", "8080", null));
        AISResponse aisResponse = (AISResponse) connection.sendAndReceive(new AISRequest());
        return aisResponse.ais();
/* With this code included, test crashes with CLIENTS > 1
        networkHandler.disconnectWorker();
        NetworkHandlerFactory.closeNetwork();
*/
    }

    private AkibaInformationSchema readAISFromMySQL() throws Exception
    {
        // Read ais from mysql
        Source mysqlSource = new MySQLSource(getDatabaseHost(), getDatabasePort(), getRootUserName(), getRootPassword());
        return new Reader(mysqlSource).load();
    }

    private static final Executor executor = Executors.newCachedThreadPool();

    private static class ServerEventHandler implements CommEventNotifier
    {
        @Override
        public void onConnect(AkibaNetworkHandler networkHandler)
        {
            executor.execute(new MessageHandler(NettyAkibanConnectionImpl.createConnection(networkHandler)));
        }

        @Override
        public void onDisconnect(AkibaNetworkHandler networkHandler)
        {
        }

        public ServerEventHandler(AkibaInformationSchema ais)
        {
            this.ais = ais;
        }

        private AkibaInformationSchema ais;

        private class MessageHandler implements Runnable
        {
            @Override
            public void run()
            {
                while (true) {
                    ByteBuffer payload = null;
                    try {
                        AISRequest aisRequest = (AISRequest) connection.receive();
                        connection.send(new AISResponse(ais));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            public MessageHandler(AkibanConnection connection)
            {
                this.connection = connection;
            }

            private final AkibanConnection connection;
        }
    }
}
