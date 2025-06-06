package ie.voltdb.xdcrreader;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.stream.api.Sinks;
import org.voltdb.stream.api.kafka.KafkaStartingOffset;
import org.voltdb.stream.api.kafka.KafkaStreamSourceConfigurator;
import org.voltdb.stream.api.pipeline.VoltPipeline;
import org.voltdb.stream.api.pipeline.VoltStreamBuilder;
import org.voltdb.xdcrutil.XdcrActionType;
import org.voltdb.xdcrutil.XdcrUtils;
import org.voltdb.xdcrutil.filters.*;
import org.voltdb.xdcrutil.converters.*;

public class VoltXdcrConflictReaderPipeline implements VoltPipeline  {

    final String SOURCE_STREAMS = "voltdbexportVOLTDB_AUTOGEN_XDCR_CONFLICTS_PARTITIONED";
    final String KAFKASERVER = "10.13.1.20:9092";
    final String VOLTSERVER = "10.13.1.21";

    Client c = null;
    HashMap<String, List<String>> pkMap;

    public VoltXdcrConflictReaderPipeline() {
        try {
            c = connectVoltDB(VOLTSERVER);
            pkMap = XdcrUtils.getPKs(c);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":Reader    :" + message);

    }
    
    @Override
    public void define(VoltStreamBuilder stream) {

        stream.withName("xdcr_conflicts")
                .consumeFromSource(KafkaStreamSourceConfigurator.aConsumer()
                        .withGroupId("ConflictReader" + System.currentTimeMillis()).withTopicNames(SOURCE_STREAMS)
                        .withBootstrapServers(KAFKASERVER).withStartingOffset(KafkaStartingOffset.EARLIEST)
                        .withPollTimeout(Duration.ofMillis(250)).withMaxCommitRetries(3)
                        .withMaxCommitTimeout(Duration.ofSeconds(10)))
                .processWith(new XDCRMessageKafkaToXdcrConflictMessage())
                .processWith(new XDCRAcceptedChangeFilter(false))
                .processWith(new XDCRActionTypeFilter(XdcrActionType.U))
                .processWith(new XDCRMessageProcessor(this))
                .processWith(new XDCRConflictMessageToStringConverter())
                .terminateWithSink(Sinks.stdout());
    }


    @Override
    public void finalize() {

        if (c != null) {
            try {
                msg("Draining Volt connection...");
                c.drain();
            } catch (NoConnectionsException | InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                msg("Closing Volt connection...");
                c.close();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            c = null;
        }
    }
}
