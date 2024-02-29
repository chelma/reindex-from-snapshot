package com.rfs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SnapshotRepoData {
    public static SnapshotRepoData fromRepoFile(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SnapshotRepoData data = mapper.readValue(new File(filePath), SnapshotRepoData.class);
        return data;
    }

    public List<Snapshot> snapshots;
    public Map<String, RawIndex> indices;
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

    public static class RawIndex {
        public String id;
        public List<String> snapshots;
        @JsonProperty("shard_generations")
        public List<String> shardGenerations;
    }

    public static class Index {
        public static Index fromRawIndex(String name, RawIndex rawIndex) {
            Index index = new Index();
            index.name = name;
            index.id = rawIndex.id;
            index.snapshots = rawIndex.snapshots;
            index.shardGenerations = rawIndex.shardGenerations;
            return index;
        }

        public String name;
        public String id;
        public List<String> snapshots;
        public List<String> shardGenerations;
    }
}

