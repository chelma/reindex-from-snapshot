package com.rfs.common;

import org.apache.lucene.codecs.CodecUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

public class ShardMetadata {
    public static interface Factory {
        private JsonNode getJsonNode(SnapshotRepo.Provider repoDataProvider, String snapshotId, String indexId, int shardId, SmileFactory smileFactory) throws Exception {
            String shardDirPath = repoDataProvider.getSnapshotDirPath() + "/indices/" + indexId + "/" + shardId;
            String filePath = shardDirPath + "/snap-" + snapshotId + ".dat";

            try (InputStream fis = new FileInputStream(new File(filePath))) {
                // Don't fully understand what the value of this code is, but it progresses the stream so we need to do it
                // See: https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/repositories/blobstore/ChecksumBlobStoreFormat.java#L100
                byte[] bytes = fis.readAllBytes();
                ByteArrayIndexInput indexInput = new ByteArrayIndexInput("index-metadata", bytes);
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, "snapshot", 1, 1);
                int filePointer = (int) indexInput.getFilePointer();
                InputStream bis = new ByteArrayInputStream(bytes, filePointer, bytes.length - filePointer);

                ObjectMapper smileMapper = new ObjectMapper(smileFactory);
                return smileMapper.readTree(bis);
            }
        }

        default ShardMetadata.Data fromSnapshotRepoDataProvider(SnapshotRepo.Provider repoDataProvider, String snapshotName, String indexName, int shardId) throws Exception {
            SmileFactory smileFactory = getSmileFactory();
            String snapshotId = repoDataProvider.getSnapshotId(snapshotName);
            String indexId = repoDataProvider.getIndexId(indexName);
            JsonNode root = getJsonNode(repoDataProvider, snapshotId, indexId, shardId, smileFactory);            
            return fromJsonNode(root, indexId, indexName, shardId);
        }
        public ShardMetadata.Data fromJsonNode(JsonNode root, String indexId, String indexName, int shardId) throws Exception;
        public SmileFactory getSmileFactory();
    }

    public static interface Data {
        public String getSnapshotName();    
        public String getIndexName();    
        public String getIndexId();    
        public int getShardId();    
        public int getIndexVersion();    
        public long getStartTime();    
        public long getTime();    
        public int getNumberOfFiles();    
        public long getTotalSize();
        public List<FileInfo> getFiles();
    }

    public static interface FileInfo {
        public String getName();
        public String getPhysicalName();
        public long getLength();
        public String getChecksum();
        public long getPartSize();
        public String getWrittenBy();
        public String getMetaHash();
        public long getNumberOfParts();
        public String partName(long part);
    }
    
}
