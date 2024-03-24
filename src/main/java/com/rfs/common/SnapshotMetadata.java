package com.rfs.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;


public class SnapshotMetadata {

    public static interface Factory {
        private JsonNode getJsonNode(SnapshotRepo.Provider repoDataProvider, String snapshotName, SmileFactory smileFactory) throws Exception {
            String snapshotId = repoDataProvider.getSnapshotId(snapshotName);

            if (snapshotId == null) {
                throw new Exception("Snapshot not found");
            }

            String filePath = repoDataProvider.getSnapshotDirPath().toString() + "/snap-" + snapshotId + ".dat";

            try (InputStream fis = new FileInputStream(new File(filePath))) {
                // Don't fully understand what the value of this code is, but it progresses the stream so we need to do it
                // See: https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/repositories/blobstore/ChecksumBlobStoreFormat.java#L100
                byte[] bytes = fis.readAllBytes();
                ByteArrayIndexInput indexInput = new ByteArrayIndexInput("snapshot-metadata", bytes);
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, "snapshot", 1, 1);
                int filePointer = (int) indexInput.getFilePointer();
                InputStream bis = new ByteArrayInputStream(bytes, filePointer, bytes.length - filePointer);

                ObjectMapper smileMapper = new ObjectMapper(smileFactory);
                return smileMapper.readTree(bis);
            }
        }

        default SnapshotMetadata.Data fromSnapshotRepoDataProvider(SnapshotRepo.Provider repoDataProvider, String snapshotName) throws Exception {
            SmileFactory smileFactory = getSmileFactory();
            JsonNode root = getJsonNode(repoDataProvider, snapshotName, smileFactory);
            return fromJsonNode(root);
        }
        public SnapshotMetadata.Data fromJsonNode(JsonNode root) throws Exception;
        public SmileFactory getSmileFactory();
    }

    /**
     * The snapshot data
     * See: https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/snapshots/SnapshotInfo.java
     */
    public static interface Data {
        public String getName();    
        public String getUuid();    
        public int getVersionId();    
        public List<String> getIndices();    
        public String getState();    
        public String getReason();    
        public boolean isIncludeGlobalState();    
        public long getStartTime();    
        public long getEndTime();    
        public int getTotalShards();    
        public int getSuccessfulShards();
        public List<?> getFailures();
    }
    
}