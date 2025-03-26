package org.apache.atlas.trino.connector;


import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdbmsEntityConnector extends AtlasEntityConnector {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsEntityConnector.class);

    @Override
    public void connectTrinoCatalog(String instanceName, String catalogName, AtlasEntity entity) throws Exception {

    }

    @Override
    public void connectTrinoSchema(String instanceName, String catalogName, String schemaName, AtlasEntity entity) throws Exception {

    }

    @Override
    public void connectTrinoTable(String instanceName, String catalogName, String schemaName, String tableName, AtlasEntity entity) throws Exception {

    }

    @Override
    public void connectTrinoColumn(String instanceName, String catalogName, String schemaName, String tableName, String columnName, AtlasEntity entity) throws Exception {

    }
}
