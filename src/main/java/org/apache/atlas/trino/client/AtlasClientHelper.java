package org.apache.atlas.trino.client;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.trino.model.Catalog;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.atlas.type.AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME;

public class AtlasClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClientHelper.class);
    public static final String TRINO_INSTANCE = "trino_instance";
    public static final String TRINO_CATALOG = "trino_catalog";
    public static final String TRINO_SCHEMA = "trino_schema";
    public static final String TRINO_TABLE = "trino_table";
    public static final String TRINO_COLUMN = "trino_column";
    public static final int pageLimit = 10000;
    private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";
    private static final String APPLICATION_PROPERTY_ATLAS_ENDPOINT = "atlas.rest.address";
    private static AtlasClientV2 atlasClientV2;

    public AtlasClientHelper(Configuration atlasConf) throws IOException {
        atlasClientV2 = getAtlasClient(atlasConf);
    }

    private AtlasClientV2 getAtlasClient(Configuration atlasConf) throws IOException {
        String[] atlasEndpoint = new String[]{DEFAULT_ATLAS_URL};

        if (atlasConf != null && ArrayUtils.isNotEmpty(atlasConf.getStringArray(APPLICATION_PROPERTY_ATLAS_ENDPOINT))) {
            atlasEndpoint = atlasConf.getStringArray(APPLICATION_PROPERTY_ATLAS_ENDPOINT);
        }

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            String[] basicAuthUsernamePassword = AuthenticationUtil.getBasicAuthenticationInput();
            return new AtlasClientV2(atlasEndpoint, basicAuthUsernamePassword);
        } else {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            return new AtlasClientV2(ugi, ugi.getShortUserName(), atlasEndpoint);
        }

    }

    public static List<AtlasEntityHeader> getAllCatalogsInInstance(String instanceGuid) throws AtlasServiceException {

        List<AtlasEntityHeader> entities = getAllRelationshipEntities(instanceGuid, "catalogs");
        if (CollectionUtils.isNotEmpty(entities)) {
            LOG.info("Retrieved {} catalogs of {} trino instance", entities.size(), instanceGuid);
            return entities;
        } else {
            LOG.info("No catalog found under {} trino instance", instanceGuid);
            return null;
        }
    }

    public static List<AtlasEntityHeader> getAllSchemasInCatalog(String catalogGuid) throws AtlasServiceException {

        List<AtlasEntityHeader> entities = getAllRelationshipEntities(catalogGuid, "schemas");
        if (CollectionUtils.isNotEmpty(entities)) {
            LOG.info("Retrieved {} schemas of {} trino catalog", entities.size(), catalogGuid);
            return entities;
        } else {
            LOG.info("No schema found under {} trino catalog", catalogGuid);
            return null;
        }
    }

    public static List<AtlasEntityHeader> getAllTablesInSchema(String schemaGuid) throws AtlasServiceException {

        List<AtlasEntityHeader> entities = getAllRelationshipEntities(schemaGuid, "tables");
        if (CollectionUtils.isNotEmpty(entities)) {
            LOG.info("Retrieved {} tables of {} trino schema", entities.size(), schemaGuid);
            return entities;
        } else {
            LOG.info("No table found under {} trino schema", schemaGuid);
            return null;
        }
    }


    public static List<AtlasEntityHeader> getAllRelationshipEntities(String entityGuid, String relationshipAttributeName) throws AtlasServiceException {

        if (entityGuid == null) {
            return null;
        }
        List<AtlasEntityHeader> entities = new ArrayList<>();
        final int pageSize = pageLimit;

        for (int i = 0; ; i++) {
            int offset = pageSize * i;
            LOG.info("Retrieving tables: offset={}, pageSize={}", offset, pageSize);

            AtlasSearchResult searchResult = atlasClientV2.relationshipSearch(entityGuid, relationshipAttributeName, null, null, true, pageSize, offset);

            List<AtlasEntityHeader> entityHeaders = searchResult == null ? null : searchResult.getEntities();
            int count = entityHeaders == null ? 0 : entityHeaders.size();

            LOG.info("Retrieved {} catalogs of {} instance", count, entityGuid);

            if (count > 0) {
                entities.addAll(entityHeaders);
            }

            if (count < pageSize) { // last page
                break;
            }
        }

        return entities;
    }


    public static AtlasEntityHeader getAtlasTrinoInstance(String namespace) throws AtlasServiceException {

        SearchParameters.FilterCriteria fc = new SearchParameters.FilterCriteria();
        fc.setAttributeName(ATTRIBUTE_QUALIFIED_NAME);
        fc.setAttributeValue(namespace);
        fc.setOperator(SearchParameters.Operator.EQ);
        fc.setCondition(SearchParameters.FilterCriteria.Condition.AND);
        LOG.info("Searching for instance : {}", namespace);

        AtlasSearchResult searchResult = atlasClientV2.basicSearch(TRINO_INSTANCE, fc, null, null, true, 25, 0);

        List<AtlasEntityHeader> entityHeaders = searchResult == null ? null : searchResult.getEntities();
        if (CollectionUtils.isNotEmpty(entityHeaders)) {
            return entityHeaders.get(0);
        }
        return null;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo findEntity(final String typeName, final String qualifiedName, boolean minExtInfo, boolean ignoreRelationship) throws AtlasServiceException {
        AtlasEntity.AtlasEntityWithExtInfo ret = null;

        try {
            ret = atlasClientV2.getEntityByAttribute(typeName, Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName), minExtInfo, ignoreRelationship);
        } catch (AtlasServiceException e) {
            if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return null;
            }

            throw e;
        }
        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createEntity(AtlasEntity.AtlasEntityWithExtInfo entity) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("creating {} entity: {}", entity.getEntity().getTypeName(), entity);
        }

        AtlasEntity.AtlasEntityWithExtInfo ret = null;
        EntityMutationResponse response = atlasClientV2.createEntity(entity);
        List<AtlasEntityHeader> createdEntities = response.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

        if (CollectionUtils.isNotEmpty(createdEntities)) {
            for (AtlasEntityHeader createdEntity : createdEntities) {
                if (ret == null) {
                    ret = atlasClientV2.getEntityByGuid(createdEntity.getGuid());

                    LOG.info("Created {} entity: name={}, guid={}", ret.getEntity().getTypeName(), ret.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), ret.getEntity().getGuid());
                } else if (ret.getEntity(createdEntity.getGuid()) == null) {
                    AtlasEntity.AtlasEntityWithExtInfo newEntity = atlasClientV2.getEntityByGuid(createdEntity.getGuid());

                    ret.addReferredEntity(newEntity.getEntity());

                    if (MapUtils.isNotEmpty(newEntity.getReferredEntities())) {
                        for (Map.Entry<String, AtlasEntity> entry : newEntity.getReferredEntities().entrySet()) {
                            ret.addReferredEntity(entry.getKey(), entry.getValue());
                        }
                    }

                    LOG.info("Created {} entity: name={}, guid={}", newEntity.getEntity().getTypeName(), newEntity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME), newEntity.getEntity().getGuid());
                }
            }
        }

        clearRelationshipAttributes(ret);

        return ret;
    }

    public static void deleteByGuid(Set<String> guidTodelete) throws AtlasServiceException {

        if (CollectionUtils.isNotEmpty(guidTodelete)) {

            for (String guid : guidTodelete) {
                EntityMutationResponse response = atlasClientV2.deleteEntityByGuid(guid);

                if (response.getDeletedEntities().size() < 1) {
                    LOG.info("Entity with guid : {} is not deleted", guid);
                } else {
                    LOG.info("Entity with guid : {} is deleted", guid);
                }
            }
        } else {
            LOG.info("No Entity to delete from Atlas");
        }
    }

    private static void clearRelationshipAttributes(AtlasEntity.AtlasEntityWithExtInfo entity) {
        if (entity != null) {
            clearRelationshipAttributes(entity.getEntity());

            if (entity.getReferredEntities() != null) {
                clearRelationshipAttributes(entity.getReferredEntities().values());
            }
        }
    }

    private static void clearRelationshipAttributes(Collection<AtlasEntity> entities) {
        if (entities != null) {
            for (AtlasEntity entity : entities) {
                clearRelationshipAttributes(entity);
            }
        }
    }

    private static void clearRelationshipAttributes(AtlasEntity entity) {
        if (entity != null && entity.getRelationshipAttributes() != null) {
            entity.getRelationshipAttributes().clear();
        }
    }


    public static AtlasEntity.AtlasEntityWithExtInfo createTrinoInstanceEntity(String trinoNamespace) throws Exception {
        String qualifiedName = trinoNamespace;
        AtlasEntity.AtlasEntityWithExtInfo ret = findEntity(TRINO_INSTANCE, qualifiedName, true, true);

        if (ret == null) {
            ret = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity entity = new AtlasEntity(TRINO_INSTANCE);

            entity.setAttribute("qualifiedName", qualifiedName);
            entity.setAttribute("name", trinoNamespace);

            ret.setEntity(entity);
            ret = createEntity(ret);
        }

        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createTrinoCatalogEntity(Catalog catalog) throws Exception {
        String catalogName    = catalog.getName();
        String trinoNamespace = catalog.getInstanceName();

        AtlasEntity.AtlasEntityWithExtInfo ret = findEntity(TRINO_CATALOG, catalogName + "@" + trinoNamespace, true, true);
        if (ret == null) {
            ret = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity entity = new AtlasEntity(TRINO_CATALOG);

            entity.setAttribute("qualifiedName", catalogName + "@" + trinoNamespace);
            entity.setAttribute("name", catalogName);
            entity.setAttribute("connectorType", catalog.getType());
            entity.setRelationshipAttribute(TRINO_INSTANCE, AtlasTypeUtil.getAtlasRelatedObjectId(catalog.getTrinoInstanceEntity().getEntity(), "trino_instance_catalog"));

            if (catalog.getConnector() != null) {
                catalog.getConnector().connectTrinoCatalog(catalog.getHookInstanceName(), catalogName, entity);
            }
            ret.setEntity(entity);
            ret = createEntity(ret);
        }

        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createTrinoSchemaEntity(Catalog catalog, AtlasEntity catalogEntity, String schema) throws Exception {
        String qualifiedName = catalog.getName() + "." + schema + "@" + catalog.getInstanceName();

        AtlasEntity.AtlasEntityWithExtInfo ret = findEntity(TRINO_SCHEMA, qualifiedName, true, true);

        if (ret == null) {
            ret = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity entity = new AtlasEntity(TRINO_SCHEMA);

            entity.setAttribute("qualifiedName", qualifiedName);
            entity.setAttribute("name", schema);
            entity.setRelationshipAttribute("catalog", AtlasTypeUtil.getAtlasRelatedObjectId(catalogEntity, "trino_schema_catalog"));

            if (catalog.getConnector() != null) {
                catalog.getConnector().connectTrinoSchema(catalog.getHookInstanceName(), catalog.getName(), schema, entity);
            }

            ret.setEntity(entity);
            ret = createEntity(ret);
        }

        return ret;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo getTrinoTableEntity(Catalog catalog, String schema, String table, AtlasEntity schemaEntity) throws Exception {
        String qualifiedName = catalog.getName() + "." + schema + "." + table + "@" + catalog.getInstanceName();

        AtlasEntity.AtlasEntityWithExtInfo ret = findEntity(TRINO_TABLE, qualifiedName, true, true);

        if (ret == null) {
            ret = new AtlasEntity.AtlasEntityWithExtInfo();
            AtlasEntity entity = new AtlasEntity(TRINO_TABLE);

            entity.setAttribute("qualifiedName", qualifiedName);
            entity.setAttribute("name", table);
            entity.setAttribute("type", "BASE_TABLE");

            entity.setRelationshipAttribute("schema", AtlasTypeUtil.getAtlasRelatedObjectId(schemaEntity, "trino_table_schema"));

            if (catalog.getConnector() != null) {
                catalog.getConnector().connectTrinoTable(catalog.getHookInstanceName(), catalog.getName(), schema, table, entity);
            }
            ret.setEntity(entity);
        }

        return ret;
    }


    public static AtlasEntity getTrinoColumnEntity(Catalog catalog, String schema, String tableName, Map.Entry<String, Map<String, Object>> columnEntry, AtlasEntity tableEntity) throws Exception {
        AtlasEntity entity = new AtlasEntity(TRINO_COLUMN);

        String columnName = columnEntry.getKey();
        String qualifiedName = catalog.getName() + "." + schema + "." + tableName + "." + columnName + "@" + catalog.getInstanceName();

        entity.setAttribute("qualifiedName", qualifiedName);
        entity.setAttribute("name", columnName);
        if (MapUtils.isNotEmpty(columnEntry.getValue())) {
            Map<String, Object> columnAttr = columnEntry.getValue();
            entity.setAttribute("data_type", columnAttr.get("data_type"));
            entity.setAttribute("position", columnAttr.get("ordinal_position"));
            entity.setAttribute("default_value", columnAttr.get("column_default"));
            entity.setAttribute("isNullable", columnAttr.get("is_nullable"));
        }

        entity.setRelationshipAttribute("table", AtlasTypeUtil.getAtlasRelatedObjectId(tableEntity, "trino_table_columns"));

        if (catalog.getConnector() != null) {
            catalog.getConnector().connectTrinoColumn(catalog.getHookInstanceName(), catalog.getName(), schema, tableName, columnName, entity);
        }
        return entity;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo createTrinoTableEntity(Catalog catalog, AtlasEntity.AtlasEntityWithExtInfo tableEntityExt, AtlasEntity schemaEntity, List<AtlasEntity> columnEntities) throws Exception {
        AtlasEntity entity = tableEntityExt.getEntity();

        entity.setRelationshipAttribute("columns", AtlasTypeUtil.getAtlasRelatedObjectIds(columnEntities, "trino_table_columns"));

        tableEntityExt.addReferredEntity(schemaEntity);
        if (columnEntities != null) {
            for (AtlasEntity column : columnEntities) {
                tableEntityExt.addReferredEntity(column);
            }
        }

        tableEntityExt.setEntity(entity);
        return createEntity(tableEntityExt);
    }

    public static void close() {
        atlasClientV2.close();
    }

}
