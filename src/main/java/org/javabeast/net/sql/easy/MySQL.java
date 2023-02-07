package org.javabeast.net.sql.easy;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author JavaBeast
 * @version 1.0
 */
public class MySQL {

    private final String url, user, password;

    private Connection connection;

    /**
     * @param url the url to connect to
     * @param user the username
     * @param password the password
     */
    public MySQL(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     *
     * @param ip the ip of the server with the database on it
     * @param database the database to connect to, set to "" if you don't want to use a specific database
     * @param user the username for database login
     * @param password the password for database login
     */
    public MySQL(String ip, String database, String user, String password) {
        this.url = "jdbc:mysql://" + ip + ":3306/" + database;
        this.user = user;
        this.password = password;
    }

    /**
     * Connect to the database
     * @return Object containing the result (success, connection time, errors, the connection)
     */
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

    /**
     * Create a new Database
     * @param name the name of the new database
     * @return a query response
     */
    public QueryResponse CREATE_DATABASE(String name) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("CREATE DATABASE ").append(name);

        return getQueryResponse(start, query);
    }

    /**
     * Delete a Database
     * @param name the name of the database
     * @return a query response
     */
    public QueryResponse DROP_DATABASE(String name) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("DROP DATABASE ").append(name);

        return getQueryResponse(start, query);
    }

    /**
     * Create a backup of a database
     * @param database the database to make the backup from
     * @param bak the path to a .bak file where the backup will be saved
     * @return a query response
     */
    public QueryResponse BACKUP_DATABASE(String database, String bak) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("BACKUP DATABASE ").append(database).append(" TO DISK = '").append(bak).append("'");

        return getQueryResponse(start, query);
    }

    private QueryResponse getQueryResponse(long start, StringBuilder query) {
        ConnectionResponse connectionResponse = connect();
        if (connect().connected()) {
            return getQueryResponse(start, query, connectionResponse);
        } else {
            String[] errors = new String[connectionResponse.errors.length + 1];
            errors[errors.length - 1] = "not connected";
            return new QueryResponse(false, new Date().getTime() - start, errors, query.toString(), this);
        }
    }

    private QueryResponse getQueryResponse(long start, StringBuilder query, ConnectionResponse connectionResponse) {
        Connection connection = connectionResponse.connection;
        try {
            connection.prepareStatement(query.toString()).execute();
            return new QueryResponse(true, new Date().getTime() - start, new String[0], query.toString(), this);
        } catch (Exception exception) {
            return new QueryResponse(false, new Date().getTime() - start, new String[]{exception.getMessage()}, query.toString(), this);
        }
    }

    /**
     * Create a new Table
     * @param ifNotExists set to 'true' if the table should only be created if it doesn't exist
     * @param name the name of the table
     * @param tableDataObjects all the columns with their datatype
     * @return a query response
     */
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
            query.append(tableDataObject.name()).append(" ").append(tableDataObject.type().toString());

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
            return getQueryResponse(start, query, connectionResponse);
        } else {
            String[] errors = new String[connectionResponse.errors.length + 1];
            errors[errors.length - 1] = "not connected";
            return new QueryResponse(false, new Date().getTime() - start, errors, query.toString(), this);
        }
    }

    /**
     * delete a table
     * @param name the name of the table
     * @return a query response
     */
    public QueryResponse DROP_TABLE(String name) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("DROP TABLE ").append(name);

        return getQueryResponse(start, query);
    }

    /**
     * Insert a new object into a table
     * @param table the table to insert the object into
     * @param entries the object (as key-value-pairs) to add to the database
     * @return a query response
     */
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

        return getQueryResponse(start, query, entries);
    }

    /**
     * update an existing object in the database
     * @param table the table, where the object is listed
     * @param set the keys and values to be changed
     * @param where the keys which have to be set to the values for the update to be applied
     * @return a query response
     */
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

        return getQueryResponse(start, query, pairs);
    }

    /**
     * Delete objects from a table
     * @param table the table to delete the object(s) from
     * @param where keys, which have to be set to a specific value, for the element to be deleted
     * @return a query response
     */
    public QueryResponse DELETE(String table, TableEntryPair[] where) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("DELETE FROM ").append(table).append(" WHERE ");
        if (where == null || where.length == 0) query.append("1");
        else {
            for(int i = 0; i < where.length; i++) {
                query.append(where[i]).append("=?");
                if (i < where.length - 1) query.append(" AND ");
            }
        }

        return getQueryResponse(start, query, where);
    }

    /**
     * Select elements from the database
     * @param columns a list of columns which should be selected from the database, null or empty to select everything
     * @param table the table to select the objects from
     * @param where a list of attributes which have to be set, for the element to be selected
     * @return a query result
     */
    public QueryResult SELECT(String[] columns, String table, TableEntryPair[] where) {
        long start = new Date().getTime();

        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        if (columns == null || columns.length == 0) query.append("*");
        else {
            for (int i = 0; i < columns.length; i++) {
                query.append(columns[i]);
                if (i < columns.length - 1) query.append(",");
            }
        }

        query.append(" FROM ").append(table).append(" WHERE ");
        if (where == null || where.length == 0) query.append("1");
        else {
            for (int i = 0; i < where.length; i++) {
                query.append(where[i].key).append("=?");
                if (i < where.length - 1) query.append(" AND ");
            }
        }

        query.append(";");

        ConnectionResponse connectionResponse = connect();
        if (connectionResponse.connected()) {
            Connection connection = connectionResponse.connection;
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(query.toString());

                if (where != null) {
                    for (int i = 0; i < where.length; i++) {
                        preparedStatement.setObject(i + 1, where[i].value);
                    }
                }

                ResultSet resultSet = preparedStatement.executeQuery();
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int cols = rsmd.getColumnCount();

                List<TableEntry> entries = new ArrayList<>();
                while (resultSet.next()) {
                    List<TableEntryPair> list = new ArrayList<>();
                    for (int i = 0; i < cols; i++) {
                        list.add(new TableEntryPair(rsmd.getColumnLabel(i + 1), resultSet.getObject(i + 1)));
                    }
                    TableEntryPair[] tableEntryPairs = list.toArray(new TableEntryPair[0]);
                    entries.add(new TableEntry(tableEntryPairs));
                }

                Table tbl = new Table(entries.toArray(new TableEntry[0]), this);
                return new QueryResult(true, new Date().getTime()- start, tbl, new String[0], query.toString(), this);
            } catch (Exception exception) {
                return new QueryResult(false, new Date().getTime() - start, new Table(null, this), new String[]{exception.getMessage()}, query.toString(), this);
            }
        } else {
            String[] err = new String[connectionResponse.errors.length + 1];
            err[0] = connectionResponse.errors[0];
            err[1] = "not connected";

            return new QueryResult(false, new Date().getTime() - start, new Table(null, this), err, query.toString(), this);
        }
    }

    /**
     * Disconnect from the database
     */
    public void disconnect() {
        try {
            if (this.connection != null && !this.connection.isClosed()) connection.close();
        } catch (Exception ignored) {}
    }

    private QueryResponse getQueryResponse(long start, StringBuilder query, TableEntryPair[] pairs) {
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

    /**
     * will be returned when connecting to the database
     * @param connected true if connected to the database
     * @param time the time passed during the connection process
     * @param errors a list of errors, occurred during the connection process, will be empty if connected is true
     * @param connection the connection
     */
    public record ConnectionResponse(boolean connected, long time, String[] errors, Connection connection) { }

    /**
     * returned by none SELECT actions
     * @param success true if the query got executed successfully
     * @param time the time passed during the connection process
     * @param errors a list of errors, occurred during the connection process, will be empty if success is true
     * @param sql the sql which got executed
     * @param mySQL this object
     */
    public record QueryResponse(boolean success, long time, String[] errors, String sql, MySQL mySQL) {
        /**
         * Close the connection to the database
         */
        public void close() {
            mySQL.disconnect();
        }
    }

    /**
     * returned by SELECT actions
     * @param success true if the query was executed successfully
     * @param time the time passed during the connection process
     * @param table a table with all the received results
     * @param errors a list of errors, occurred, during the connection process, will be empty if success is true
     * @param sql the sql which got executed
     * @param mySQL this object
     */
    public record QueryResult(boolean success, long time, Table table, String[] errors, String sql, MySQL mySQL) {
        /**
         * Close the connection to the database
         */
        public void close() {mySQL.disconnect();}
    }

    /**
     * The attributes for a table column
     * @param name the name of the column
     * @param type the type of the column
     * @param arguments the arguments
     * @param primary true if this should be the primary column
     */
    public record TableDataObject(String name, DataType type, Argument[] arguments, boolean primary) { }

    /**
     * A table holding all the returned entries from a database
     * @param tableEntries the entries
     * @param mySQL this object
     */
    public record Table(TableEntry[] tableEntries, MySQL mySQL) { }

    /**
     * a database object with a key-value-pair list, containing columns with their values
     * @param tableEntries the key-value-pair list
     */
    public record TableEntry(TableEntryPair[] tableEntries) { }

    /**
     * A column with a value
     * @param key the key
     * @param value the value
     */
    public record TableEntryPair(String key, Object value) { }

    /**
     * Create a MIN(column) statement
     * @param column the column name
     */
    public record Min(String column) {
        /**
         * used in the select statement
         * @return MIN(column) AS column
         */
        public String toString() {
            return "MIN(" + column + ") AS " + column;
        }
    }

    /**
     * Create a MAX(column) statement
     * @param column the column name
     */
    public record Max(String column) {
        /**
         * used in the select statement
         * @return MAX(column) AS column
         */
        public String toString() {
            return "MAX(" + column + ") AS " + column;
        }
    }

    /**
     * a columns data type
     * @param type the type
     * @param length the length
     */
    public record DataType(Type type, int length) {
        public String toString() {
            String string = type.name();
            if (length > 0) string += "(" + length + ")";
            return string;
        }
    }

    /**
     * all available mysql data types
     */
    public enum Type {
        TINYINT,
        SMALLINT,
        MEDIUMINT,
        INT,
        BIGINT,
        DECIMAL,
        FLOAT,
        DOUBLE,
        REAL,
        BIT,
        BOOLEAN,
        SERIAL,
        DATE,
        DATETIME,
        TIMESTAMP,
        TIME,
        YEAR,
        CHAR,
        VARCHAR,
        TINYTEXT,
        TEXT,
        MEDIUMTEXT,
        LONGTEXT,
        BINARY,
        VARBINARY,
        TINYBLOB,
        MEDIUMBLOB,
        BLOB,
        LONGBLOB,
        ENUM,
        SET,
        GEOMETRY,
        POINT,
        LINESTRING,
        POLYGON,
        MULTIPOINT,
        MULTILINESTRING,
        MULTIPOLYGON,
        GEOMETRYCOLLECTION,
        JSON
    }

    /**
     * additional arguments for the columns
     */
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
