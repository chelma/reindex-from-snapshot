package com.rfs;

import java.util.List;

import org.apache.logging.log4j.core.util.JsonUtils;

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

    public final String snapshotName;
    public final String indexName;
    public final String indexId;
    public final int shardId;
    public final int indexVersion;
    public final long startTime;
    public final long time;
    public final int numberOfFiles;
    public final long totalSize;
    public final List<FileMetadata> files;

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
        public final String name;
        public final String physicalName;
        public final long length;
        public final String checksum;
        public final long partSize;
        public final String writtenBy;
        public final String metaHash;

        @JsonCreator
        public FileMetadata(
                @JsonProperty("name") String name,
                @JsonProperty("physical_name") String physicalName,
                @JsonProperty("length") long length,
                @JsonProperty("checksum") String checksum,
                @JsonProperty("part_size") long partSize,
                @JsonProperty("written_by") String writtenBy,
                @JsonProperty("meta_hash") String metaHash) {
            this.name = name;
            this.physicalName = physicalName;
            this.length = length;
            this.checksum = checksum;
            this.partSize = partSize;
            this.writtenBy = writtenBy;
            this.metaHash = metaHash;
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
