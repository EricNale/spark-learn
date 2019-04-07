package com.lucas.boot.spark.job;

public class MongoConnect {
    public String getHost() {
        return host;
    }

    private final String host;
    private final String username;
    private final String password;
    private final String collection;
    private final String dbName;

    public MongoConnect(String host, String username, String password, String collection, String dbName) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.collection = collection;
        this.dbName = dbName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getCollection() {
        return collection;
    }

    public String getDbName() {
        return dbName;
    }
}
