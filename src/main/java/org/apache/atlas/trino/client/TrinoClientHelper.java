package org.apache.atlas.trino.client;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrinoClientHelper {

    private static String jdbcUrl;
    private static String username;
    private static String password;

    public TrinoClientHelper(Configuration atlasConf) {
        this.jdbcUrl             = atlasConf.getString("atlas.trino.jdbc.address");
        this.username            = atlasConf.getString("atlas.trino.jdbc.user");
        this.password            = atlasConf.getString("atlas.trino.jdbc.password", "");
    }

    public static Connection getTrinoConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    public Map<String, String> getAllTrinoCatalogs()  {
        Map<String, String> catalogs = new HashMap<>();
        try {
            Connection connection = getTrinoConnection();
            Statement stmt = connection.createStatement();
            StringBuilder query = new StringBuilder();
            query.append("SELECT catalog_name, connector_name FROM system.metadata.catalogs");

            ResultSet rs = stmt.executeQuery(query.toString());
            while (rs.next()) {
                catalogs.put(rs.getString("catalog_name"), rs.getString("connector_name"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return catalogs;
    }

    public List<String> getTrinoSchemas(String catalog, String schemaToImport) throws SQLException {
        List<String> schemas  = new ArrayList<>();
        Connection connection = getTrinoConnection();
        Statement stmt        = connection.createStatement();
        StringBuilder query   = new StringBuilder();
        query.append("SELECT schema_name FROM " + catalog + ".information_schema.schemata");

        if (StringUtils.isNotEmpty(schemaToImport)) {
            query.append(" where schema_name = '" + schemaToImport + "'");
        }

        ResultSet rs = stmt.executeQuery(query.toString());
        while (rs.next()) {
            schemas.add(rs.getString("schema_name"));
        }

        return schemas;
    }

    public List<String> getTrinoTables(String catalog, String schema, String tableToImport) throws SQLException {
        List<String> tables = new ArrayList<>();
        Connection connection = getTrinoConnection();
        Statement stmt = connection.createStatement();
        StringBuilder query   = new StringBuilder();
        query.append("SELECT table_name FROM " + catalog + ".information_schema.tables WHERE table_schema = '" + schema + "'");
        if (StringUtils.isNotEmpty(tableToImport)) {
            query.append(" where table_name = '" + tableToImport + "'");
        }

        ResultSet rs = stmt.executeQuery(query.toString());
        while (rs.next()) {
            tables.add(rs.getString("table_name"));
        }

        return tables;
    }

    public Map<String, Map<String, Object>> getTrinoColumns(String catalog, String schema, String table) throws SQLException {
        Map<String,Map<String, Object>> columns = new HashMap<>();
        Connection connection = getTrinoConnection();
        Statement stmt = connection.createStatement();
        StringBuilder query   = new StringBuilder();
        query.append("SELECT column_name, ordinal_position, column_default, is_nullable, data_type FROM " + catalog + ".information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = '"+ table + "'" );

        ResultSet rs = stmt.executeQuery(query.toString());
        while (rs.next()) {
            Map<String, Object> columnMetadata = new HashMap<>();
            columnMetadata.put("ordinal_position",rs.getInt("ordinal_position"));
            columnMetadata.put("column_default",rs.getString("column_default"));
            columnMetadata.put("column_name",rs.getString("column_name"));
            if (StringUtils.isNotEmpty(rs.getString("is_nullable"))) {
                if (StringUtils.equalsIgnoreCase(rs.getString("is_nullable"), "YES")) {
                    columnMetadata.put("is_nullable", true);
                } else {
                    columnMetadata.put("is_nullable", false);
                }
            }
            columnMetadata.put("data_type",rs.getString("data_type"));

            columns.put(rs.getString("column_name"), columnMetadata);
        }

        return columns;
    }
}
