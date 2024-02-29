package com.rfs;

public class ConnectionDetails {
    public final String host;
    public final int port;
    public final String username;
    public final String password;

    public ConnectionDetails(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }
}
