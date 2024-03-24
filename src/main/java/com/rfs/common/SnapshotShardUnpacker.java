package com.rfs.common;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.BytesRef;

import com.rfs.version_es_6_8.ElasticsearchConstants_ES_6_8;

public class SnapshotShardUnpacker {
    public static void unpack(ShardMetadata.Data shardMetadata, Path snapshotBasePath, Path luceneFilesBasePath) throws Exception {
        // Some constants
        NativeFSLockFactory lockFactory = NativeFSLockFactory.INSTANCE;

        // Get the path to the shard's blob files
        Path shardDirPath = Paths.get(snapshotBasePath + "/indices/" + shardMetadata.getIndexId() + "/" + shardMetadata.getShardId());

        // Create the directory for the shard's lucene files
        Path luceneIndexDir = Paths.get(luceneFilesBasePath + "/" + shardMetadata.getIndexName() + "/" + shardMetadata.getShardId());
        Files.createDirectories(luceneIndexDir);
        final FSDirectory primaryDirectory = FSDirectory.open(luceneIndexDir, lockFactory);

        for (ShardMetadata.FileInfo fileMetadata : shardMetadata.getFiles()) {
            System.out.println("Unpacking - Blob Name: " + fileMetadata.getName() + ", Lucene Name: " + fileMetadata.getPhysicalName());
            IndexOutput indexOutput = primaryDirectory.createOutput(fileMetadata.getPhysicalName(), IOContext.DEFAULT);

            if (fileMetadata.getName().startsWith("v__")) {
                final BytesRef hash = fileMetadata.getMetaHash();
                indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
            } else {
                try (InputStream stream = new PartSliceStream(shardDirPath, fileMetadata)) {
                    final byte[] buffer = new byte[Math.toIntExact(Math.min(ElasticsearchConstants_ES_6_8.BUFFER_SIZE_IN_BYTES, fileMetadata.getLength()))];
                    int length;
                    while ((length = stream.read(buffer)) > 0) {
                        indexOutput.writeBytes(buffer, 0, length);
                    }
                }
            }
            indexOutput.close();
        }        
    }
    
}
