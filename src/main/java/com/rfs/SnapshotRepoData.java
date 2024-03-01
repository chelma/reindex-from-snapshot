package com.rfs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SnapshotRepoData {
    private static Path findRepoFile(Path dir) throws IOException {
        Pattern pattern = Pattern.compile("^index-\\d+$");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry) && pattern.matcher(entry.getFileName().toString()).matches()) {
                    return entry; // There should only be one match
                }
            }
        }
        return null; // No match found
    }

    public static SnapshotRepoData fromRepoFile(Path filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SnapshotRepoData data = mapper.readValue(new File(filePath.toString()), SnapshotRepoData.class);
        data.filePath = filePath;
        return data;
    }

    public static SnapshotRepoData fromRepoDir(Path dir) throws IOException {
        Path file = findRepoFile(dir);
        if (file == null) {
            throw new IOException("No index file found in " + dir);
        }
        return fromRepoFile(file);
    }

    public Path filePath;
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

