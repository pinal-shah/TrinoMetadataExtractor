package org.apache.atlas.trino.connector;

import org.apache.atlas.model.instance.AtlasEntity;

public abstract class AtlasEntityConnector {

    public abstract void connectTrinoCatalog(String instanceName, String catalogName, AtlasEntity entity) throws Exception;

    public abstract void connectTrinoSchema(String instanceName,String catalogName, String schemaName, AtlasEntity entity) throws Exception;

    public abstract void connectTrinoTable(String instanceName, String catalogName, String schemaName, String tableName, AtlasEntity entity) throws Exception;

    public abstract void connectTrinoColumn(String instanceName, String catalogName, String schemaName,String tableName, String columnName, AtlasEntity entity) throws Exception;


}
