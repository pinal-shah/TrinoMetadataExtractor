package org.apache.atlas.trino.cli;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.trino.client.AtlasClientHelper;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.trino.model.Catalog;
import org.apache.atlas.trino.client.TrinoClientHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ExtractorService {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractorService.class);
    private static Configuration atlasProperties;
    private static TrinoClientHelper trinoClientHelper;
    private static AtlasClientHelper atlasClientHelper;
    private static String trinoNamespace;
    private ExtractorContext context;
    private static final String TRINO_CATALOG_REGISTERED = "atlas.trino.catalog.registered";
    public static final int THREAD_POOL_SIZE = 10000;


    public boolean execute(ExtractorContext context) throws Exception {
        this.context = context;
        atlasProperties = context.getAtlasConf();
        trinoClientHelper = context.getTrinoConnector();
        atlasClientHelper = context.getAtlasConnector();
        trinoNamespace = context.getNamespace();

        Map<String, String> catalogs = trinoClientHelper.getAllTrinoCatalogs();
        LOG.info("Found {} catalogs in Trino", catalogs.toString());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            try {
                processCatalogs(context, catalogs);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        executor.submit(() -> {
            try {
                deleteCatalogs(context, catalogs);
            } catch (AtlasServiceException e) {
                throw new RuntimeException(e);
            }
        });
        executor.shutdown();
        return true;
    }


    public void processCatalogs(ExtractorContext context, Map<String, String> catalogInTrino) throws Exception {
        List<Catalog> catalogsToProcess = new ArrayList<>();

        if (StringUtils.isEmpty(context.getCatalog())) {
            catalogsToProcess = getCatalogsToProcess(catalogInTrino);
            if (CollectionUtils.isEmpty(catalogsToProcess)) {
                LOG.warn("No catalogs found to process");
            }
        } else {
            if (catalogInTrino.containsKey(context.getCatalog())) {
                Catalog catalog = getCatalogInstance(context.getCatalog(), catalogInTrino.get(context.getCatalog()));
                catalog.setSchemaToImport(context.getSchema());
                catalog.setTableToImport(context.getTable());
                catalogsToProcess.add(catalog);
            } else {
                LOG.error("catalog {} not found", context.getCatalog());
            }
        }

        if (CollectionUtils.isEmpty(catalogsToProcess)) {
            return;
        }

        AtlasEntity.AtlasEntityWithExtInfo trinoInstanceEntity = atlasClientHelper.createTrinoInstanceEntity(trinoNamespace);

        Queue<Catalog> catalogQueue = new ConcurrentLinkedQueue<>(catalogsToProcess);
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            executorService.submit(() -> {
                Catalog currentCatalog;
                while ((currentCatalog = catalogQueue.poll()) != null) {
                    try {
                        currentCatalog.setTrinoInstanceEntity(trinoInstanceEntity);
                        processCatalog(currentCatalog);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        executorService.shutdown();
    }

    private static List<Catalog> getCatalogsToProcess(Map<String, String> catalogInTrino) {
        List<Catalog> catalogsToProcess = new ArrayList<>();

        if (MapUtils.isEmpty(catalogInTrino)) {
            return catalogsToProcess;
        }

        String registeredCatalogs = atlasProperties.getString(TRINO_CATALOG_REGISTERED);
        List<String> registeredCatalogList = Arrays.stream(registeredCatalogs.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        for (String registeredCatalog : registeredCatalogList) {
            if (catalogInTrino.containsKey(registeredCatalog)) {
                catalogsToProcess.add(getCatalogInstance(registeredCatalog, catalogInTrino.get(registeredCatalog)));
            } else {
                LOG.error("catalog not found");
            }
        }

        return catalogsToProcess;
    }

    private static Catalog getCatalogInstance(String catalogName, String connectorType) {
        if (catalogName == null) return null;

        boolean isHookEnabled = atlasProperties.getBoolean("atlas.trino.catalog.hook.enabled." + catalogName);
        String hookNamespace = null;
        if (isHookEnabled) {
            hookNamespace = atlasProperties.getString("atlas.trino.catalog.hook.enabled." + catalogName + ".namespace");
        }
        Catalog catalog = new Catalog(catalogName, connectorType, isHookEnabled, hookNamespace, trinoNamespace);
        return catalog;
    }

    private static Set<String> getCatalogsToDelete(Map<String, String> catalogInTrino, String instanceGuid) throws AtlasServiceException {

        if (instanceGuid != null) {

            List<AtlasEntityHeader> catalogsInAtlas = atlasClientHelper.getAllCatalogsInInstance(instanceGuid);
            if (catalogsInAtlas != null) {

                if (catalogInTrino == null) {
                    catalogInTrino = new HashMap<>();
                }

                Map<String, String> finalCatalogInTrino = catalogInTrino;
                return catalogsInAtlas.stream()
                        .filter(entity -> entity.getAttribute("name") != null) // Ensure "name" attribute exists
                        .filter(entity -> !finalCatalogInTrino.containsKey(entity.getAttribute("name"))) // Only missing schemas
                        .map(AtlasEntityHeader::getGuid) // Extract GUIDs
                        .collect(Collectors.toSet());
            }
        }

        return new HashSet<>();
    }

    private static Set<String> getSchemasToDelete(List<String> schemasInTrino, String catalogGuid) throws AtlasServiceException {

        if (catalogGuid != null) {

            List<AtlasEntityHeader> schemasInAtlas = atlasClientHelper.getAllSchemasInCatalog(catalogGuid);
            if (schemasInAtlas != null) {

                if (schemasInTrino == null) {
                    schemasInTrino = new ArrayList<>();
                }

                List<String> finalSchemasInTrino = schemasInTrino;
                return schemasInAtlas.stream()
                        .filter(entity -> entity.getAttribute("name") != null) // Ensure "name" attribute exists
                        .filter(entity -> !finalSchemasInTrino.contains(entity.getAttribute("name"))) // Only missing schemas
                        .map(AtlasEntityHeader::getGuid) // Extract GUIDs
                        .collect(Collectors.toSet());
            }
        }

        return new HashSet<>();
    }

    private static Set<String> getTablesToDelete(List<String> tablesInTrino, String schemaGuid) throws AtlasServiceException {

        if (schemaGuid != null) {

            List<AtlasEntityHeader> tablesInAtlas = atlasClientHelper.getAllTablesInSchema(schemaGuid);
            if (tablesInAtlas != null) {

                if (tablesInTrino == null) {
                    tablesInTrino = new ArrayList<>();
                }

                List<String> finalTablesInTrino = tablesInTrino;
                return tablesInAtlas.stream()
                        .filter(entity -> entity.getAttribute("name") != null) // Ensure "name" attribute exists
                        .filter(entity -> !finalTablesInTrino.contains(entity.getAttribute("name"))) // Only missing schemas
                        .map(AtlasEntityHeader::getGuid) // Extract GUIDs
                        .collect(Collectors.toSet());
            }
        }

        return new HashSet<>();
    }

    public void processCatalog(Catalog catalog) throws Exception {
        if (catalog != null) {
            String catalogName = catalog.getName();

            // create trino_catalog
            AtlasEntity.AtlasEntityWithExtInfo trinoCatalogEntity = atlasClientHelper.createTrinoCatalogEntity(catalog);

            List<String> schemas = trinoClientHelper.getTrinoSchemas(catalogName, catalog.getSchemaToImport());
            LOG.info("Found {} schema under {} catalog", schemas.size(), catalogName);

            processSchemas(catalog, trinoCatalogEntity.getEntity(), schemas);

            if (StringUtils.isNotEmpty(context.getSchema())) {
                deleteSchemas(schemas, trinoCatalogEntity.getEntity().getGuid());
            }
        }
    }

    public void processSchemas(Catalog catalog, AtlasEntity trinoCatalogEntity, List<String> schemaToImport) throws Exception {
        for (String schemaName : schemaToImport) {
            AtlasEntity.AtlasEntityWithExtInfo schemaEntity = atlasClientHelper.createTrinoSchemaEntity(catalog, trinoCatalogEntity, schemaName);

            List<String> tables = trinoClientHelper.getTrinoTables(catalog.getName(), schemaName, catalog.getTableToImport());
            LOG.info("Found {} tables under {}.{} catalog.schema", tables.size(), catalog.getName(), schemaName);

            processTables(catalog, schemaName, schemaEntity.getEntity(), tables);

            if (StringUtils.isNotEmpty(context.getTable())) {
                deleteTables(tables, schemaEntity.getEntity().getGuid());
            }
        }
    }

    public void processTables(Catalog catalog, String schemaName, AtlasEntity schemaEntity, List<String> tablesToImport) throws Exception {
        for (String tableName : tablesToImport) {

            AtlasEntity.AtlasEntityWithExtInfo tableEntityExt = atlasClientHelper.getTrinoTableEntity(catalog, schemaName, tableName, schemaEntity);
            AtlasEntity tableEntity = tableEntityExt.getEntity();

            Map<String, Map<String, Object>> columns = trinoClientHelper.getTrinoColumns(catalog.getName(), schemaName, tableName);
            LOG.info("Found {} columns under {}.{}.{} catalog.schema.table", columns.size(), catalog.getName(), schemaName, tableName);

            List<AtlasEntity> columnEntities = new ArrayList<>();
            for (Map.Entry<String, Map<String, Object>> entry : columns.entrySet()) {
                AtlasEntity columnEntity = atlasClientHelper.getTrinoColumnEntity(catalog, schemaName, tableName, entry, tableEntity);
                columnEntities.add(columnEntity);
            }

            atlasClientHelper.createTrinoTableEntity(catalog, tableEntityExt, schemaEntity, columnEntities);

        }
    }

    public void deleteCatalogs(ExtractorContext context, Map<String, String> catalogInTrino) throws AtlasServiceException {
        if (StringUtils.isEmpty(context.getCatalog())) {
            return;
        }
        AtlasEntityHeader trinoInstance = atlasClientHelper.getAtlasTrinoInstance(trinoNamespace);

        Set<String> catalogsToDelete = getCatalogsToDelete(catalogInTrino, trinoInstance.getGuid());
        for (String catalogGuid : catalogsToDelete) {
            deleteSchemas(null, catalogGuid);
            atlasClientHelper.deleteByGuid(catalogsToDelete);
        }
    }

    public void deleteSchemas(List<String> schemasInTrino, String catalogGuid) throws AtlasServiceException {
        Set<String> schemasToDelete = getSchemasToDelete(schemasInTrino, catalogGuid);
        for (String schemaGuid : schemasToDelete) {
            deleteTables(null, schemaGuid);
            atlasClientHelper.deleteByGuid(schemasToDelete);
        }
    }

    private void deleteTables(List<String> tablesInTrino, String schemaGuid) throws AtlasServiceException {
        Set<String> tablesToDelete = getTablesToDelete(tablesInTrino, schemaGuid);
        if (CollectionUtils.isNotEmpty(tablesToDelete)) {
            atlasClientHelper.deleteByGuid(tablesToDelete);
        }
    }
}
