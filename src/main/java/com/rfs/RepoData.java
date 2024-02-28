package com.rfs;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class RepoData {
    public List<Snapshot> snapshots;
    public Map<String, Index> indices;
    @JsonProperty("min_version")
    public String minVersion;
    @JsonProperty("index_metadata_identifiers")
    public Map<String, String> indexMetadataIdentifiers;

    public static class Snapshot {
        public String name;
        public String uuid;
        public int state;
        @JsonProperty("index_metadata_lookup")
        public Map<String, String> indexMetadataLookup;
        public String version;
    }

    public static class Index {
        public String id;
        public List<String> snapshots;
        @JsonProperty("shard_generations")
        public List<String> shardGenerations;
    }
}

