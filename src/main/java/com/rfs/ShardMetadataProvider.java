package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class ShardMetadataProvider {
    private final String indexName;
    private final BlobStoreIndexShardSnapshot snapshot;
    private final Path snapshotDirPath;
    private final Path shardDirPath;
    private final int shardId;

    public static ShardMetadataProvider fromSnapshotRepoDataProvider(SnapshotRepoDataProvider repoDataProvider, String snapshotName, String indexName, int shardId) throws Exception {
        String snapshotId = repoDataProvider.getSnapshotId(snapshotName);
        String indexId = repoDataProvider.getIndexId(indexName);
        Path shardDirPath = Paths.get(repoDataProvider.getSnapshotDirPath() + "/indices/" + indexId + "/" + shardId);
        BlobStoreIndexShardSnapshot snapshot = readIndexShardMetadata(repoDataProvider.getSnapshotDirPath(), shardDirPath, snapshotId);

        return new ShardMetadataProvider(snapshot, repoDataProvider.getSnapshotDirPath(), indexName, shardDirPath, shardId);
    }

    private static BlobStoreIndexShardSnapshot readIndexShardMetadata(Path snapshotDirPath, Path shardDirPath, String snapshotId) throws Exception {
        BlobPath blobPath = new BlobPath().add(shardDirPath.toString());

        FsBlobStore blobStore = new FsBlobStore(
            ElasticsearchConstants.BUFFER_SIZE_IN_BYTES,
            snapshotDirPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat =
            new ChecksumBlobStoreFormat<>("snapshot", "snap-%s.dat", BlobStoreIndexShardSnapshot::fromXContent);

        BlobStoreIndexShardSnapshot shardSnapshot = indexShardSnapshotFormat.read(container, snapshotId, ElasticsearchConstants.EMPTY_REGISTRY);

        blobStore.close();

        return shardSnapshot;
    }


    public ShardMetadataProvider(BlobStoreIndexShardSnapshot snapshot, Path snapshotDirPath, String indexName, Path shardDirPath, int shardId) {
        this.snapshot = snapshot;
        this.snapshotDirPath = snapshotDirPath;
        this.indexName = indexName;
        this.shardDirPath = shardDirPath;
        this.shardId = shardId;
    }

    public List<BlobStoreIndexShardSnapshot.FileInfo> getFiles() {
        return snapshot.indexFiles();
    }

    public String getIndexName() {
        return indexName;
    }

    public Path getSnapshotDirPath() {
        return snapshotDirPath;
    }

    public Path getShardDirPath() {
        return shardDirPath;
    }

    public int getShardId() {
        return shardId;
    }
}
