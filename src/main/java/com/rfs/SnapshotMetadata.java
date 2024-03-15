package com.rfs;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The snapshot data; see [1]
 * 
 * [1] https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/snapshots/SnapshotInfo.java
 */
public class SnapshotMetadata {
    /**
     * A version of the Elasticsearch approach simplified by assuming JSON; see here [1] for more details.
     * 
     * [1] https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/snapshots/SnapshotInfo.java#L583
     */
    public static SnapshotMetadata fromJsonNode(JsonNode root) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNodeRoot = (ObjectNode) root;
        SnapshotMetadata snapshotMetadata = mapper.treeToValue(objectNodeRoot.get("snapshot"), SnapshotMetadata.class);
        return snapshotMetadata;
    }

    private String name;
    private String uuid;
    @JsonProperty("version_id")
    private int versionId;
    private List<String> indices;
    private String state;
    private String reason;
    @JsonProperty("include_global_state")
    private boolean includeGlobalState;
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("end_time")
    private long endTime;
    @JsonProperty("total_shards")
    private int totalShards;
    @JsonProperty("successful_shards")
    private int successfulShards;
    private List<?> failures; // Haven't looked at this yet

    public String getName() {
        return name;
    }

    public String getUuid() {
        return uuid;
    }

    public int getVersionId() {
        return versionId;
    }

    public List<String> getIndices() {
        return indices;
    }

    public String getState() {
        return state;
    }

    public String getReason() {
        return reason;
    }

    public boolean isIncludeGlobalState() {
        return includeGlobalState;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public int getSuccessfulShards() {
        return successfulShards;
    }

    public List<?> getFailures() {
        return failures;
    }
}
