package com.rfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Collectors;

public class RestClient {
    public final ConnectionDetails connectionDetails;

    public RestClient(ConnectionDetails connectionDetails) {
        this.connectionDetails = connectionDetails;
    }

    private String getConnectionString() {
        return "http://" + connectionDetails.host + ":" + connectionDetails.port;
    }
    
    public void put(String path, String body) throws Exception {
        String urlString = getConnectionString() + "/" + path;

        System.out.println(urlString);
        System.out.println(body);

        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        // Construct the basic auth header value
        String auth = connectionDetails.username + ":" + connectionDetails.password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        String authHeaderValue = "Basic " + new String(encodedAuth);

        // Set the request method to PUT
        conn.setRequestMethod("PUT");

        // Set the necessary headers
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Authorization", authHeaderValue);

        // Enable input and output streams
        conn.setDoOutput(true);

        // Write the request body
        try(OutputStream os = conn.getOutputStream()) {
            byte[] input = body.getBytes("utf-8");
            os.write(input, 0, input.length);           
        }        

        // Get the response code to determine success
        int responseCode = conn.getResponseCode();
        System.out.println("Response Code: " + responseCode);
        System.out.println("Response Message: " + conn.getResponseMessage());

        String responseBody;
        if (responseCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
            // Read error stream if present
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                responseBody = br.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        } else {
            // Read input stream for successful requests
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                responseBody = br.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }


        System.out.println("Response Body: " + responseBody);

        conn.disconnect();        
    }
}
