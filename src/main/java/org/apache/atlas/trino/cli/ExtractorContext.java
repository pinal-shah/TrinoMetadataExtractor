package org.apache.atlas.trino.cli;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.trino.client.AtlasClientHelper;
import org.apache.atlas.trino.client.TrinoClientHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;

public class ExtractorContext {

    private String namespace;
    private String catalog;
    private String schema;
    private String table;
    private final Configuration atlasConf;
    private AtlasClientHelper atlasClientHelper;
    private TrinoClientHelper trinoClientHelper;
    private String cronExpression;

    static final String OPTION_CATALOG_SHORT             = "c";
    static final String OPTION_CATALOG_LONG              = "catalog";
    static final String OPTION_SCHEMA_SHORT              = "s";
    static final String OPTION_SCHEMA_LONG               = "schema";
    static final String OPTION_TABLE_SHORT               = "t";
    static final String OPTION_TABLE_LONG                = "table";
    static final String OPTION_CRON_EXPRESSION_SHORT     = "cx";
    static final String OPTION_CRON_EXPRESSION_LONG      = "cronExpression";
    static final String OPTION_FAIL_ON_ERROR             = "failOnError";
    static final String OPTION_DELETE_NON_EXISTING       = "deleteNonExisting";
    static final String OPTION_HELP_SHORT                = "h";
    static final String OPTION_HELP_LONG                 = "help";


    public ExtractorContext(CommandLine cmd) throws AtlasException, IOException {
       this.atlasConf = getAtlasProperties();
       this.atlasClientHelper = createAtlasClientHelper();
       this.trinoClientHelper = createTrinoClientHelper();
       this.namespace = atlasConf.getString("atlas.trino.namespace", "cm");
       this.catalog = cmd.getOptionValue(OPTION_CATALOG_SHORT);
       this.schema = cmd.getOptionValue(OPTION_SCHEMA_SHORT);
       this.table = cmd.getOptionValue(OPTION_TABLE_SHORT);
       this.cronExpression = cmd.getOptionValue(OPTION_CRON_EXPRESSION_SHORT);
    }

    public Configuration getAtlasConf() {
        return atlasConf;
    }

    public AtlasClientHelper getAtlasConnector() {
        return atlasClientHelper;
    }

    public TrinoClientHelper getTrinoConnector() {
        return trinoClientHelper;
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    private Configuration getAtlasProperties() throws AtlasException {
        return ApplicationProperties.get();
    }

    private TrinoClientHelper createTrinoClientHelper() {
        return new TrinoClientHelper(atlasConf);
    }

    private AtlasClientHelper createAtlasClientHelper() throws IOException {
        return new AtlasClientHelper(atlasConf);
    }

}
