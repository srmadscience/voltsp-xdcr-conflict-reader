package ie.voltdb.xdcrreader;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.xdcrutil.XdcrUtils;

public class VoltXdcrConflictReaderWrangler  {

    final String SOURCE_STREAMS = "voltdbexportVOLTDB_AUTOGEN_XDCR_CONFLICTS_PARTITIONED";
    final String KAFKASERVER = "10.13.1.20:9092";
    final String VOLTSERVER = "10.13.1.21";

    Client c = null;
    HashMap<String, List<String>> pkMap;

    public VoltXdcrConflictReaderWrangler() {
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
