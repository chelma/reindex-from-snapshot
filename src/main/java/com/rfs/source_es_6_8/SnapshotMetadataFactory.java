package com.rfs.source_es_6_8;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.lucene.codecs.CodecUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rfs.common.ByteArrayIndexInput;

public class SnapshotMetadataFactory {

    public static SnapshotMetadata fromSnapshotRepoDataProvider(SnapshotRepoDataProvider repoDataProvider, String snapshotName) throws Exception {
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

            ObjectMapper smileMapper = new ObjectMapper(ElasticsearchConstants.SMILE_FACTORY);
            JsonNode jsonNode = smileMapper.readTree(bis);

            return SnapshotMetadata.fromJsonNode(jsonNode);
        }
    }    
}
