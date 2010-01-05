/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.akiba.cserver;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import com.akiba.message.MessageRegistryBase;
import com.akiba.network.AkibaNetworkHandler;
import com.akiba.network.CommEventNotifier;
import com.akiba.network.NetworkHandlerFactory;
/**
 *
 * @author pbeaman
 */
public class CServer {

	private static final Logger LOGGER = Logger.getLogger(CServer.class.getName());

	public static class ChannelNotifier implements CommEventNotifier {

		private volatile int connectCounter = 0;

		@Override
		public void onConnect(AkibaNetworkHandler handler) {
			int counter = ++connectCounter;
			System.out.println("Connection #" + connectCounter + " created");
			LOGGER.info("Connection #" + connectCounter + " created");
			new Thread(new CServerRunnable(handler), "CServer_" + counter)
					.start();
		}

		@Override
		public void onDisconnect(AkibaNetworkHandler handler) {

		}
	}

	/**
	 * A Runnable that reads Network messages, acts on them and returns results.
	 *
	 * @author peter
	 *
	 */
	private static class CServerRunnable implements Runnable {

		private final AkibaNetworkHandler handler;

		public CServerRunnable(final AkibaNetworkHandler handler) {
			this.handler = handler;
		}

		public void run() {

            try {
                final ByteBuffer bb = handler.getMsg();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
	}

	private static class MessageRegistry extends MessageRegistryBase
    {
	    public MessageRegistry()
	    {
            maxTypeCode(1000);
            register(1, WriteRowMessage.class.getName());
	    }
	}

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {
		ChannelNotifier callback = new ChannelNotifier();
		NetworkHandlerFactory.initializeNetwork("localhost", "8080",
				(CommEventNotifier) callback);
		new MessageRegistry();
		try {
			//
			// For now this is "crash-only software" - there is no
			// graceful shutdown. Just kill or ctrl-c the process.
			//
			while (true) {
				Thread.sleep(5000);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		// TODO code application logic here
		System.out.println("test this");
		NetworkHandlerFactory.closeNetwork();
	}

}
