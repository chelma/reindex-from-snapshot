package com.rfs;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

/**
 * The information about a snapshot that we care about for our purposes.  This is a subset of the information that
 * Elasticsearch stores; see here [1] for more details.
 * 
 * [1] https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/snapshots/SnapshotInfo.java
 */
public class SnapshotMetadata {

    public enum State {
        SUCCESS, FAILED, IN_PROGRESS, PARTIAL, INCOMPATIBLE
    }

    private static final String NAME = "name";
    private static final String UUID = "uuid";
    private static final String VERSION_ID = "version_id";
    private static final String STATE = "state";
    private static final String REASON = "reason";
    private static final String INDICES = "indices";
    private static final String TOTAL_SHARDS = "total_shards";
    private static final String SUCCESSFUL_SHARDS = "successful_shards";
    private static final String INCLUDE_GLOBAL_STATE = "include_global_state";


    public final String name;
    public final String uuid;
    public final int versionId;
    public final State state;
    public final String reason;
    public final List<String> indices;
    public final int totalShards;
    public final int successfulShards;
    public final Boolean includeGlobalState;

    /**
     * A simplified version of the Elasticsearch approach; see here [1] for more details.
     * 
     * [1] https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/snapshots/SnapshotInfo.java#L583
     */
    public static SnapshotMetadata fromJsonNode(JsonNode root) throws Exception {
        // Initialize the metadata fields
        String name = null;
        String uuid = null;
        int versionId = 0;
        State state = null;
        String reason = null;
        List<String> indices = new ArrayList<>();
        int totalShards = 0;
        int successfulShards = 0;
        Boolean includeGlobalState = null;
        
        return null;

        // return new SnapshotMetadata(name, uuid, versionId, state, reason, indices, totalShards, successfulShards, includeGlobalState);
    }



    /**
     * A simplified version of the Elasticsearch approach; see here [1] for more details.
     * 
     * [1] https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/snapshots/SnapshotInfo.java#L583
     */
    public static SnapshotMetadata fromParser(SmileParser parser) throws Exception {
        // Initialize the metadata fields
        String name = null;
        String uuid = null;
        int versionId = 0;
        State state = null;
        String reason = null;
        List<String> indices = new ArrayList<>();
        int totalShards = 0;
        int successfulShards = 0;
        Boolean includeGlobalState = null;
        
        // Parse the stream        
        JsonToken nextToken = parser.nextToken();
        while (nextToken != JsonToken.END_OBJECT) {
            if (nextToken == JsonToken.FIELD_NAME) {
                String currentFieldName = parser.getCurrentName();
                nextToken = parser.nextToken();
                if (nextToken == JsonToken.VALUE_NULL) {
                    continue;
                }
                switch (currentFieldName) {
                    case NAME:
                        name = parser.getText();
                        break;
                    case UUID:
                        uuid = parser.getText();
                        break;
                    case VERSION_ID:
                        versionId = parser.getIntValue();
                        break;
                    case STATE:
                        state = State.valueOf(parser.getText());
                        break;
                    case REASON:
                        reason = parser.getText();
                        break;
                    case INDICES:
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            indices.add(parser.getText());
                        }
                        break;
                    case TOTAL_SHARDS:
                        totalShards = parser.getIntValue();
                        break;
                    case SUCCESSFUL_SHARDS:
                        successfulShards = parser.getIntValue();
                        break;
                    case INCLUDE_GLOBAL_STATE:
                        includeGlobalState = parser.getBooleanValue();
                        break;
                    default:
                        System.out.println("Skipping field: " + currentFieldName);
                }
            }
            nextToken = parser.nextToken();
        }

        return new SnapshotMetadata(name, uuid, versionId, state, reason, indices, totalShards, successfulShards, includeGlobalState);
    }

    public SnapshotMetadata(String name, String uuid, int versionId, State state, String reason, List<String> indices, int totalShards, int successfulShards, Boolean includeGlobalState) {
        this.name = name;
        this.uuid = uuid;
        this.versionId = versionId;
        this.state = state;
        this.reason = reason;
        this.indices = indices;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.includeGlobalState = includeGlobalState;
    }
}
