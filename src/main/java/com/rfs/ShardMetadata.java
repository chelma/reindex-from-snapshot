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
            List<FileMetadataRaw> files) {
        this.snapshotName = snapshotName;
        this.indexName = indexName;
        this.indexId = indexId;
        this.shardId = shardId;
        this.indexVersion = indexVersion;
        this.startTime = startTime;
        this.time = time;
        this.numberOfFiles = numberOfFiles;
        this.totalSize = totalSize;

        // Convert the raw file metadata to the FileMetadata class
        List<FileMetadata> convertedFiles = new java.util.ArrayList<>();
        for (FileMetadataRaw fileMetadataRaw : files) {
            convertedFiles.add(FileMetadata.fromFileMetadataRaw(fileMetadataRaw));
        }
        this.files = convertedFiles;
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
        public final List<FileMetadataRaw> files;

        @JsonCreator
        public ShardMetadataRaw(
                @JsonProperty("name") String name,
                @JsonProperty("index_version") int indexVersion,
                @JsonProperty("start_time") long startTime,
                @JsonProperty("time") long time,
                @JsonProperty("number_of_files") int numberOfFiles,
                @JsonProperty("total_size") long totalSize,
                @JsonProperty("files") List<FileMetadataRaw> files) {
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
        private String physicalName;
        private long length;
        private String checksum;
        private long partSize;
        private long numberOfParts;
        private String writtenBy;
        private String metaHash;

        public static FileMetadata fromFileMetadataRaw(FileMetadataRaw fileMetadataRaw) {
            return new FileMetadata(
                    fileMetadataRaw.name,
                    fileMetadataRaw.physicalName,
                    fileMetadataRaw.length,
                    fileMetadataRaw.checksum,
                    fileMetadataRaw.partSize,
                    fileMetadataRaw.writtenBy,
                    fileMetadataRaw.metaHash
            );
        }

        public FileMetadata(
                String name,
                String physicalName,
                long length,
                String checksum,
                long partSize,
                String writtenBy,
                String metaHash) {
            this.name = name;
            this.physicalName = physicalName;
            this.length = length;
            this.checksum = checksum;
            this.partSize = partSize;
            this.writtenBy = writtenBy;
            this.metaHash = metaHash;

            // Calculate the number of parts the file is chopped into; taken from Elasticsearch code
            // See: https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/index/snapshots/blobstore/BlobStoreIndexShardSnapshot.java#L68
            long partBytes = Long.MAX_VALUE;
            if (partSize != Long.MAX_VALUE) {
                partBytes = partSize;
            }

            long totalLength = length;
            long numberOfParts = totalLength / partBytes;
            if (totalLength % partBytes > 0) {
                numberOfParts++;
            }
            if (numberOfParts == 0) {
                numberOfParts++;
            }
            this.numberOfParts = numberOfParts;
        }

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

        public long getNumberOfParts() {
            return numberOfParts;
        }

        // The Snapshot file may be split into multiple blobs; use this to find the correct file name
        public String partName(long part) {
            if (numberOfParts > 1) {
                return name + ".part" + part;
            } else {
                return name;
            }
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

    private static class FileMetadataRaw {
        public final String name;
        public final String physicalName;
        public final long length;
        public final String checksum;
        public final long partSize;
        public final String writtenBy;
        public final String metaHash;

        @JsonCreator
        public FileMetadataRaw(
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
    }
}
