package org.javabeast.net.sql.easy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Date;

public class MySQL {

    private final String url, user, password;


    private Connection connection;

    public MySQL(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public MySQL(String ip, String database, String user, String password) {
        this.url = "jdbc:mysql://" + ip + ":3306/" + database;
        this.user = user;
        this.password = password;
    }

    public ConnectionResponse connect() {
        long start = new Date().getTime();
        try {
            if (this.connection != null && !this.connection.isClosed()) return new ConnectionResponse(true, -1, new String[0], this.connection);

            this.connection = DriverManager.getConnection(this.url, this.user, this.password);

            return new ConnectionResponse(true, new Date().getTime() - start, new String[0], this.connection);
        } catch (Exception exception) {
            return new ConnectionResponse(false, new Date().getTime() - start, new String[]{exception.getMessage()}, null);
        }
    }

    public QueryResponse CREATE_TABLE(boolean ifNotExists, String name, TableDataObject[] tableDataObjects) {
        long start = new Date().getTime();
        StringBuilder query = new StringBuilder();
        query.append("CREATE TABLE ");
        if (ifNotExists) query.append("IF NOT EXISTS ");

        query.append(name).append("(");

        String primary = "";
        for (int i = 0; i < tableDataObjects.length; i++) {
            TableDataObject tableDataObject = tableDataObjects[i];
            if (tableDataObject.primary()) primary = tableDataObject.name();
            query.append(tableDataObject.name()).append(" ").append(tableDataObject.type().name());

            if (tableDataObject.arguments() != null) {
                for (Argument argument : tableDataObject.arguments()){
                    query.append(" ").append(argument.value);
                }
            }

            if (i < tableDataObjects.length - 1) query.append(",");
        }

        if (!primary.equals("")) query.append(", PRIMARY KEY (").append(primary).append(")");

        query.append(");");

        ConnectionResponse connectionResponse = connect();
        if (connectionResponse.connected()) {
            Connection connection = connectionResponse.connection;
            try {
                connection.prepareStatement(query.toString()).execute();
                return new QueryResponse(true, new Date().getTime() - start, new String[0], query.toString(), this);
            } catch (Exception exception) {
                return new QueryResponse(false, new Date().getTime() - start, new String[]{exception.getMessage()}, query.toString(), this);
            }
        } else {
            String[] errors = new String[connectionResponse.errors.length + 1];
            errors[errors.length - 1] = "not connected";
            return new QueryResponse(false, new Date().getTime() - start, errors, query.toString(), this);
        }
    }

    public QueryResponse INSERT(String table, TableEntryPair[] entries) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ").append(table);

        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (int i = 0; i < entries.length; i++) {
            TableEntryPair entryPair = entries[i];

            keys.append(entryPair.key);
            values.append("?");

            if (i < entries.length - 1) {
                keys.append(",");
                values.append(",");
            }
        }

        query.append(" (").append(keys).append(") VALUES (").append(values).append(");");

        ConnectionResponse connectionResponse = connect();
        if (connectionResponse.connected()) {
            Connection connection = connectionResponse.connection;
            try {
                PreparedStatement statement = connection.prepareStatement(query.toString());

                for (int i = 0; i < entries.length; i++) {
                    statement.setObject(i + 1, entries[i].value);
                }

                statement.execute();
                return new QueryResponse(true, new Date().getTime() - start, new String[0], query.toString(), this);
            } catch (Exception exception) {
                return new QueryResponse(false, new Date().getTime() - start, new String[]{exception.getMessage()}, query.toString(), this);
            }
        } else {
            String[] errors = new String[connectionResponse.errors.length + 1];
            errors[errors.length - 1] = "not connected";
            return new QueryResponse(false, new Date().getTime() - start, errors, query.toString(), this);
        }
    }

    public QueryResponse UPDATE(String table, TableEntryPair[] set, TableEntryPair[] where) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("UPDATE ").append(table).append(" SET ");

        if (where == null) where = new TableEntryPair[0];
        TableEntryPair[] pairs = new TableEntryPair[set.length + where.length];


        for (int i = 0; i < set.length; i++) {
            TableEntryPair pair = set[i];
            query.append("`").append(pair.key).append("`=?");
            if (i < set.length - 1) query.append(",");
            pairs[i] = pair;
        }

        if (where.length > 0) {
            query.append(" WHERE ");
            for (int i = 0; i < where.length; i++) {
                TableEntryPair pair = where[i];
                query.append("`").append(pair.key).append("`=?");
                if (i < set.length - 1) query.append(" AND ");
                pairs[set.length + i] = pair;
            }
        }

        ConnectionResponse connectionResponse = connect();
        if (connectionResponse.connected()) {
            Connection connection = connectionResponse.connection;
            try {
                PreparedStatement statement = connection.prepareStatement(query.toString());

                for (int i = 0; i < pairs.length; i++) {
                    statement.setObject(i + 1, pairs[i].value);
                }

                statement.execute();
                return new QueryResponse(true, new Date().getTime() - start, new String[0], query.toString(), this);
            } catch (Exception exception) {
                return new QueryResponse(false, new Date().getTime() - start, new String[]{exception.getMessage()}, query.toString(), this);
            }
        } else {
            String[] errors = new String[connectionResponse.errors.length + 1];
            errors[errors.length - 1] = "not connected";
            return new QueryResponse(false, new Date().getTime() - start, errors, query.toString(), this);
        }
    }

    public void disconnect() {
        try {
            if (this.connection != null && !this.connection.isClosed()) connection.close();
        } catch (Exception ignored) {}
    }

    public record ConnectionResponse(boolean connected, long time, String[] errors, Connection connection) { }
    public record QueryResponse(boolean success, long time, String[] errors, String sql, MySQL mySQL) {
        public void close() {
            mySQL.disconnect();
        }
    }
    public record TableDataObject(String name, Type type, Argument[] arguments, boolean primary) { }

    public record TableEntryPair(String key, Object value) { }

    public enum Type {
        INT,
        VARCHAR,
        JSON,
    }

    public enum Argument {
        NOT_NULL ("NOT NULL"),
        AUTO_INCREMENT ("AUTO_INCREMENT")
        ;
        public final String value;
        Argument(String value) {
            this.value = value;
        }
    }
}
