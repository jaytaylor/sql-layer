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

package com.akiban.ais.message;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.akiban.ais.BaseTestCase;
import com.akiban.ais.io.MySQLSource;
import com.akiban.ais.io.Reader;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Source;
import com.akiban.message.AkibanConnection;
import com.akiban.message.MessageRegistry;
import com.akiban.message.NettyAkibanConnectionImpl;
import com.akiban.network.AkibanNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;

public class AISMessageTest extends BaseTestCase
{
    public void test() throws Exception
    {
        if(isDatabaseAvailable() == false) {
            return;
        }
        
        MessageRegistry.initialize().registerModule("com.akiban.ais");
        AkibanInformationSchema ais = readAISFromMySQL();
        startServer(ais);
        AkibanInformationSchema ais2 = runClient();
        checkAIS(ais, ais2);
    }

    private ByteBuffer serialize(AkibanInformationSchema ais) throws Exception
    {
        ByteBuffer payload = ByteBuffer.wrap(new byte[1000000]);
        AISResponse aisResponse = new AISResponse(ais);
        aisResponse.write(payload);
        payload.flip();
        return payload;
    }

    private void checkAIS(AkibanInformationSchema ais1, AkibanInformationSchema ais2) throws Exception
    {
        assertEquals(serialize(ais1), serialize(ais2));
    }

    private void startServer(AkibanInformationSchema ais)
    {
        ServerEventHandler serverEventHandler = new ServerEventHandler(ais);
        NetworkHandlerFactory.initializeNetwork("localhost", "8080", serverEventHandler);
    }

    private AkibanInformationSchema runClient() throws Exception
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

    private AkibanInformationSchema readAISFromMySQL() throws Exception
    {
        // Read ais from mysql
        Source mysqlSource = new MySQLSource(getDatabaseHost(), getDatabasePort(), getRootUserName(), getRootPassword());
        return new Reader(mysqlSource).load();
    }

    private static final Executor executor = Executors.newCachedThreadPool();

    private static class ServerEventHandler implements CommEventNotifier
    {
        @Override
        public void onConnect(AkibanNetworkHandler networkHandler)
        {
            executor.execute(new MessageHandler(NettyAkibanConnectionImpl.createConnection(networkHandler)));
        }

        @Override
        public void onDisconnect(AkibanNetworkHandler networkHandler)
        {
        }

        public ServerEventHandler(AkibanInformationSchema ais)
        {
            this.ais = ais;
        }

        private AkibanInformationSchema ais;

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
