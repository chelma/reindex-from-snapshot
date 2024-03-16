package com.rfs;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayIndexInput;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class IndexMetadataFactory {
    public static IndexMetadata fromSnapshotRepoDataProvider(SnapshotRepoDataProvider repoDataProvider, String snapshotName, String indexName) throws Exception{
        String snapshotId = repoDataProvider.getSnapshotId(snapshotName);        
        String indexId = repoDataProvider.getIndexId(indexName);
        String indexDirPath = repoDataProvider.getSnapshotDirPath() + "/indices/" + indexId;        
        String filePath = indexDirPath + "/meta-" + snapshotId + ".dat";

        try (InputStream fis = new FileInputStream(new File(filePath))) {
            // Don't fully understand what the value of this code is, but it progresses the stream so we need to do it
            // See: https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/repositories/blobstore/ChecksumBlobStoreFormat.java#L100
            byte[] bytes = fis.readAllBytes();
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("index-metadata", bytes);
            CodecUtil.checksumEntireFile(indexInput);
            CodecUtil.checkHeader(indexInput, "index-metadata", 1, 1);
            int filePointer = (int) indexInput.getFilePointer();
            InputStream bis = new ByteArrayInputStream(bytes, filePointer, bytes.length - filePointer);

            ObjectMapper smileMapper = new ObjectMapper(ElasticsearchConstants.SMILE_FACTORY);
            JsonNode jsonNode = smileMapper.readTree(bis);

            return IndexMetadata.fromJsonNode(jsonNode, indexId, indexName);
        }
    }
}
