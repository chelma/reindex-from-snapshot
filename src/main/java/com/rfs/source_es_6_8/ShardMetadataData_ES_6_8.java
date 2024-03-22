package com.rfs.source_es_6_8;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rfs.common.ShardMetadata;


public class ShardMetadataData_ES_6_8 implements com.rfs.common.ShardMetadata.Data {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String snapshotName;
    private String indexName;
    private String indexId;
    private int shardId;
    private int indexVersion;
    private long startTime;
    private long time;
    private int numberOfFiles;
    private long totalSize;
    private List<FileInfo> files;

    public ShardMetadataData_ES_6_8(
            String snapshotName,
            String indexName,
            String indexId,
            int shardId,
            int indexVersion,
            long startTime,
            long time,
            int numberOfFiles,
            long totalSize,
            List<FileInfoRaw> files) {
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
        List<FileInfo> convertedFiles = new java.util.ArrayList<>();
        for (FileInfoRaw fileMetadataRaw : files) {
            convertedFiles.add(FileInfo.fromFileMetadataRaw(fileMetadataRaw));
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

    public List<ShardMetadata.FileInfo> getFiles() {
        List<ShardMetadata.FileInfo> convertedFiles = new ArrayList<>(files);
        return convertedFiles;
    }

    @Override
    public String toString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            return "Error converting to string: " + e.getMessage();
        }
    }

    public static class DataRaw {
        public final String name;
        public final int indexVersion;
        public final long startTime;
        public final long time;
        public final int numberOfFiles;
        public final long totalSize;
        public final List<FileInfoRaw> files;

        @JsonCreator
        public DataRaw(
                @JsonProperty("name") String name,
                @JsonProperty("index_version") int indexVersion,
                @JsonProperty("start_time") long startTime,
                @JsonProperty("time") long time,
                @JsonProperty("number_of_files") int numberOfFiles,
                @JsonProperty("total_size") long totalSize,
                @JsonProperty("files") List<FileInfoRaw> files) {
            this.name = name;
            this.indexVersion = indexVersion;
            this.startTime = startTime;
            this.time = time;
            this.numberOfFiles = numberOfFiles;
            this.totalSize = totalSize;
            this.files = files;
        }
    }

    public static class FileInfo implements ShardMetadata.FileInfo {
        private String name;
        private String physicalName;
        private long length;
        private String checksum;
        private long partSize;
        private long numberOfParts;
        private String writtenBy;
        private String metaHash;

        public static FileInfo fromFileMetadataRaw(FileInfoRaw fileMetadataRaw) {
            return new FileInfo(
                    fileMetadataRaw.name,
                    fileMetadataRaw.physicalName,
                    fileMetadataRaw.length,
                    fileMetadataRaw.checksum,
                    fileMetadataRaw.partSize,
                    fileMetadataRaw.writtenBy,
                    fileMetadataRaw.metaHash
            );
        }

        public FileInfo(
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

    public static class FileInfoRaw {
        public final String name;
        public final String physicalName;
        public final long length;
        public final String checksum;
        public final long partSize;
        public final String writtenBy;
        public final String metaHash;

        @JsonCreator
        public FileInfoRaw(
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
