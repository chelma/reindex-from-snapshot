package com.rfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;

public class SnapshotShardUnpacker {
    public static void unpack(ShardMetadataProvider shardMetadata, Path luceneFilesBasePath) throws Exception {
        // Some constants
        NativeFSLockFactory lockFactory = NativeFSLockFactory.INSTANCE;

        // Create a blob container to read the snapshot blobs
        BlobPath blobPath = new BlobPath().add(shardMetadata.getShardDirPath().toString());            
        FsBlobStore blobStore = new FsBlobStore(ElasticsearchConstants.BUFFER_SETTINGS, shardMetadata.getSnapshotDirPath(), false);
        BlobContainer container = blobStore.blobContainer(blobPath);
        
        // Create the directory for the shard's lucene files
        Path lucene_dir = Paths.get(luceneFilesBasePath + "/" + shardMetadata.getIndexName() + "/" + shardMetadata.getShardId());
        Files.createDirectories(lucene_dir);
        final FSDirectory primaryDirectory = FSDirectory.open(lucene_dir, lockFactory);
        
        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : shardMetadata.getFiles()) {
            System.out.println("Unpacking - Blob Name: " + fileInfo.name() + ", Lucene Name: " + fileInfo.metadata().name());
            IndexOutput indexOutput = primaryDirectory.createOutput(fileInfo.metadata().name(), IOContext.DEFAULT);

            if (fileInfo.name().startsWith("v__")) {
                final BytesRef hash = fileInfo.metadata().hash();
                indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
            } else {
                try (InputStream stream = new SlicedInputStream(fileInfo.numberOfParts()) {
                    @Override
                    protected InputStream openSlice(long slice) throws IOException {
                        return container.readBlob(fileInfo.partName(slice));
                    }
                }) {
                    final byte[] buffer = new byte[Math.toIntExact(Math.min(ElasticsearchConstants.BUFFER_SIZE_IN_BYTES, fileInfo.length()))];
                    int length;
                    while ((length = stream.read(buffer)) > 0) {
                        indexOutput.writeBytes(buffer, 0, length);
                    }
                }
            }
            blobStore.close();
            indexOutput.close();
        }
    }
    
}
