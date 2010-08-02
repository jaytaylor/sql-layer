package com.akiban.cserver;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.admin.Admin;
import com.akiban.admin.config.ChunkserverNetworkConfig;
import com.akiban.admin.config.ClusterConfig;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.util.AISPrinter;
import com.akiban.cserver.message.InstallAISRequest;
import com.akiban.message.AkibaConnection;
import com.akiban.message.AkibaConnectionImpl;
import com.akiban.message.ErrorResponse;
import com.akiban.message.Response;
import com.akiban.network.NetworkHandlerFactory;

class AISDistributor
{
    public AISDistributor(CServer thisChunkserver) throws IOException
    {
        try {
            LOG.info("Creating AISDistributor");
            assert thisChunkserver.isLeader();
            Admin admin = Admin.only();
            ClusterConfig cluster = admin.clusterConfig();
            Collection<ChunkserverNetworkConfig> chunkservers = cluster.chunkservers().values();
            LOG.info(String.format("all chunkservers: %s", chunkservers));
            for (ChunkserverNetworkConfig chunkserver : chunkservers) {
                if (!chunkserver.lead()) {
                    String chunkserverAddress = chunkserver.address().host().getHostAddress();
                    int chunkserverPort = chunkserver.address().port();
                    LOG.info(String.format("Connecting to %s:%s", chunkserverAddress, chunkserverPort));
                    AkibaConnection connection = AkibaConnectionImpl.createConnection
                        (NetworkHandlerFactory.getHandler(chunkserverAddress,
                                                          Integer.toString(chunkserverPort),
                                                          null));
                    cserverToConnection.put(chunkserver.name(), connection);
                }
            }
            LOG.info("AISDistributor created");
        } catch (Exception e) {
            LOG.error("Caught exception while setting up AISDistributor", e);
        }
    }

    public void distribute(AkibaInformationSchema ais) throws Exception
    {
        for (Map.Entry<String, AkibaConnection> entry : cserverToConnection.entrySet()) {
            String chunkserverName = entry.getKey();
            AkibaConnection connection = entry.getValue();
            InstallAISRequest request = new InstallAISRequest(ais);
            try {
                LOG.info(String.format("Sending AIS to %s", chunkserverName));
                LOG.debug(AISPrinter.toString(ais));
                Response response = (Response) connection.sendAndReceive(request);
                if (response.errorResponse()) {
                    LOG.error(String.format("AIS transmission failed: %s", ((ErrorResponse)response).message()));
                } else {
                    LOG.info(String.format("AIS successfully installed on %s", chunkserverName));
                }
            } catch (Exception e) {
                LOG.error(String.format("Caught exception while sending AIS to %s", chunkserverName));
                throw e;
            }
        }
    }

    private static final Log LOG = LogFactory.getLog(AISDistributor.class.getName());

    private final Map<String, AkibaConnection> cserverToConnection = new HashMap<String, AkibaConnection>();
}
