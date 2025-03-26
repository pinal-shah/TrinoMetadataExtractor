package org.apache.atlas.trino.model;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.trino.connector.AtlasEntityConnector;
import org.apache.atlas.trino.connector.ConnectorFactory;

public class Catalog {
    private String instanceName;
    private String name;
    private String type;
    private String schemaToImport;
    private String tableToImport;
    private String hookInstanceName;
    private boolean hookEnabled;
    private AtlasEntityConnector connector;
    private AtlasEntity.AtlasEntityWithExtInfo trinoInstanceEntity;

    public Catalog(String name, String type, boolean hookEnabled, String hookInstanceName, String instanceName) {
        this.name = name;
        this.type = type;
        this.hookEnabled = hookEnabled;
        this.hookInstanceName = hookInstanceName;
        this.instanceName = instanceName;
        setConnector();
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public void setConnector(){
        if (hookEnabled) {
            connector = ConnectorFactory.getConnector(type);
        }
    }

    public AtlasEntityConnector getConnector() {
        return connector;
    }

    public String getHookInstanceName() {
        return hookInstanceName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public AtlasEntity.AtlasEntityWithExtInfo getTrinoInstanceEntity() {
        return trinoInstanceEntity;
    }

    public void setTrinoInstanceEntity(AtlasEntity.AtlasEntityWithExtInfo trinoInstanceEntity) {
        this.trinoInstanceEntity = trinoInstanceEntity;
    }

    public String getTableToImport() {
        return tableToImport;
    }

    public void setTableToImport(String tableToImport) {
        this.tableToImport = tableToImport;
    }

    public String getSchemaToImport() {
        return schemaToImport;
    }

    public void setSchemaToImport(String schemaToImport) {
        this.schemaToImport = schemaToImport;
    }

}
