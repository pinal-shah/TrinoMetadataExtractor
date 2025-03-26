package org.apache.atlas.trino.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorFactory.class);

    public static AtlasEntityConnector getConnector(String connectorType) {
        switch (connectorType.toLowerCase()) {
            case "mysql":
                return new RdbmsEntityConnector();
            case "hive":
                return new HiveEntityConnector();
            default:
                LOG.warn("{} type does not have hook implemented on Atlas");
                return null;
        }
    }
}
