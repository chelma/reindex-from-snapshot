package com.rfs;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class ShardMetadata {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ShardMetadata fromJsonNode(JsonNode root, String indexId, String indexName, int shardId) throws Exception {
        ObjectNode objectNodeRoot = (ObjectNode) root;
        ShardMetadataRaw shardMetadataRaw = objectMapper.treeToValue(objectNodeRoot, ShardMetadataRaw.class);
        return new ShardMetadata(
                shardMetadataRaw.name,
                indexName,
                indexId,
                shardId,
                shardMetadataRaw.indexVersion,
                shardMetadataRaw.startTime,
                shardMetadataRaw.time,
                shardMetadataRaw.numberOfFiles,
                shardMetadataRaw.totalSize,
                shardMetadataRaw.files
        );
    }

    private String snapshotName;
    private String indexName;
    private String indexId;
    private int shardId;
    private int indexVersion;
    private long startTime;
    private long time;
    private int numberOfFiles;
    private long totalSize;
    private List<FileMetadata> files;

    public ShardMetadata(
            String snapshotName,
            String indexName,
            String indexId,
            int shardId,
            int indexVersion,
            long startTime,
            long time,
            int numberOfFiles,
            long totalSize,
            List<FileMetadata> files) {
        this.snapshotName = snapshotName;
        this.indexName = indexName;
        this.indexId = indexId;
        this.shardId = shardId;
        this.indexVersion = indexVersion;
        this.startTime = startTime;
        this.time = time;
        this.numberOfFiles = numberOfFiles;
        this.totalSize = totalSize;
        this.files = files;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexId() {
        return indexId;
    }

    public int getShardId() {
        return shardId;
    }

    public int getIndexVersion() {
        return indexVersion;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getTime() {
        return time;
    }

    public int getNumberOfFiles() {
        return numberOfFiles;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public List<FileMetadata> getFiles() {
        return files;
    }

    @Override
    public String toString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            return "Error converting to string: " + e.getMessage();
        }
    }

    private static class ShardMetadataRaw {
        public final String name;
        public final int indexVersion;
        public final long startTime;
        public final long time;
        public final int numberOfFiles;
        public final long totalSize;
        public final List<FileMetadata> files;

        @JsonCreator
        public ShardMetadataRaw(
                @JsonProperty("name") String name,
                @JsonProperty("index_version") int indexVersion,
                @JsonProperty("start_time") long startTime,
                @JsonProperty("time") long time,
                @JsonProperty("number_of_files") int numberOfFiles,
                @JsonProperty("total_size") long totalSize,
                @JsonProperty("files") List<FileMetadata> files) {
            this.name = name;
            this.indexVersion = indexVersion;
            this.startTime = startTime;
            this.time = time;
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
            this.files = files;
        }
    }

    public static class FileMetadata {
        private String name;
        @JsonProperty("physical_name")
        private String physicalName;
        private long length;
        private String checksum;
        @JsonProperty("part_size")
        private long partSize;
        @JsonProperty("written_by")
        private String writtenBy;
        @JsonProperty("meta_hash")
        private String metaHash;

        public String getName() {
            return name;
        }

        public String getPhysicalName() {
            return physicalName;
        }

        public long getLength() {
            return length;
        }

        public String getChecksum() {
            return checksum;
        }

        public long getPartSize() {
            return partSize;
        }

        public String getWrittenBy() {
            return writtenBy;
        }

        public String getMetaHash() {
            return metaHash;
        }

        @Override
        public String toString() {
            try {
                return objectMapper.writeValueAsString(this);
            } catch (Exception e) {
                return "Error converting to string: " + e.getMessage();
            }
        }
    }
}
