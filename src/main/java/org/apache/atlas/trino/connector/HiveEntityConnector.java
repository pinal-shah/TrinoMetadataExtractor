package org.apache.atlas.trino.connector;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.trino.client.AtlasClientHelper;
import org.apache.atlas.type.AtlasTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.type.AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME;

public class HiveEntityConnector extends AtlasEntityConnector {
    private static final Logger LOG = LoggerFactory.getLogger(HiveEntityConnector.class);

    public static final String HIVE_INSTANCE = "hive_instance";
    public static final String HIVE_DB = "hive_db";
    public static final String HIVE_TABLE = "hive_table";
    public static final String HIVE_COLUMN = "hive_column";

    @Override
    public void connectTrinoCatalog(String instanceName, String catalogName, AtlasEntity entity) throws Exception {

    }

    @Override
    public void connectTrinoSchema(String instanceName, String catalogName, String schemaName, AtlasEntity entity) throws Exception {
        AtlasEntity hiveDb = toDbEntity(instanceName, schemaName);
        entity.setRelationshipAttribute("hive_db", AtlasTypeUtil.getAtlasRelatedObjectId(hiveDb, "trino_schema_hive_db"));
    }

    @Override
    public void connectTrinoTable(String instanceName, String catalogName, String schemaName, String tableName, AtlasEntity entity) throws Exception {
        AtlasEntity hiveTable = toTableEntity(instanceName, schemaName, tableName);
        entity.setRelationshipAttribute("hive_table", AtlasTypeUtil.getAtlasRelatedObjectId(hiveTable, "trino_schema_hive_table"));
    }

    @Override
    public void connectTrinoColumn(String instanceName, String catalogName, String schemaName, String tableName, String columnName, AtlasEntity entity) throws Exception {
        AtlasEntity hiveColumn = toColumnEntity(instanceName, schemaName, tableName, columnName);
        entity.setRelationshipAttribute("hive_column",  AtlasTypeUtil.getAtlasRelatedObjectId(hiveColumn, "trino_schema_hive_column"));
    }

    private AtlasEntity toDbEntity(String instanceName, String schemaName) throws Exception {
        String      dbName          = schemaName;
        String      dbQualifiedName = schemaName + "@" + instanceName;
        AtlasEntity.AtlasEntityWithExtInfo ret = AtlasClientHelper.findEntity(HIVE_DB, dbQualifiedName, true, true);
        AtlasEntity hiveDb = null;
        if (ret == null || ret.getEntity() == null) {
            hiveDb = new AtlasEntity(HIVE_DB);

            hiveDb.setAttribute(ATTRIBUTE_QUALIFIED_NAME, dbQualifiedName);
            hiveDb.setAttribute("name", dbName);
        }

        return hiveDb;
    }

    private AtlasEntity toTableEntity(String instanceName, String schemaName, String tableName) throws Exception {
        String      tableQualifiedName = schemaName + "." + tableName + "@" + instanceName;
        AtlasEntity.AtlasEntityWithExtInfo ret = AtlasClientHelper.findEntity(HIVE_TABLE, tableQualifiedName, true, true);
        AtlasEntity hiveTable = null;
        if (ret == null || ret.getEntity() == null) {
            hiveTable = new AtlasEntity(HIVE_TABLE);

            hiveTable.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tableQualifiedName);
            hiveTable.setAttribute("name", tableName);
        }

        return hiveTable;
    }

    private AtlasEntity toColumnEntity(String instanceName, String schemaName, String tableName, String columnName) throws Exception {
        String      columnQualifiedName = schemaName + "." + tableName + "." + columnName + "@" + instanceName;
        AtlasEntity.AtlasEntityWithExtInfo ret = AtlasClientHelper.findEntity(HIVE_COLUMN, columnQualifiedName, true, true);
        AtlasEntity hiveColumn = null;
        if (ret == null || ret.getEntity() == null) {
            hiveColumn = new AtlasEntity(HIVE_COLUMN);

            hiveColumn.setAttribute(ATTRIBUTE_QUALIFIED_NAME, columnQualifiedName);
            hiveColumn.setAttribute("name", columnName);
            AtlasEntity hiveTable = toTableEntity(instanceName, schemaName, tableName);
            hiveColumn.setRelationshipAttribute("table", AtlasTypeUtil.getAtlasRelatedObjectId(hiveTable, "hive_table_columns"));
        }

        return hiveColumn;
    }
}
