package com.rfs;

import java.nio.file.Path;
import java.util.List;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.snapshots.SnapshotInfo;

public class SnapshotMetadataProvider {
    private final SnapshotInfo snapshotInfo;

    public static SnapshotMetadataProvider fromSnapshotRepoDataProvider(SnapshotRepoDataProvider repoDataProvider, String snapshotName) throws Exception {
        String snapshotId = repoDataProvider.getSnapshotId(snapshotName);

        if (snapshotId == null) {
            throw new Exception("Snapshot not found");
        }

        SnapshotInfo snapshotInfo = readSnapshotDetails(repoDataProvider.getSnapshotDirPath(), snapshotId);

        return new SnapshotMetadataProvider(snapshotInfo);
    }

    private static SnapshotInfo readSnapshotDetails(Path snapshotDirPath, String snapshotId) throws Exception {
        BlobPath blobPath = new BlobPath();
        blobPath.add(snapshotDirPath.toString());

        FsBlobStore blobStore = new FsBlobStore(
            ElasticsearchConstants.BUFFER_SETTINGS,
            snapshotDirPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        // See https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/repositories/blobstore/BlobStoreRepository.java#L353
        boolean compressionEnabled = false;

        ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat = new ChecksumBlobStoreFormat<>(
            "snapshot", "snap-%s.dat", SnapshotInfo::fromXContentInternal, ElasticsearchConstants.EMPTY_REGISTRY, compressionEnabled
        );

        // Read the snapshot details
        SnapshotInfo snapshotInfo = snapshotFormat.read(container, snapshotId);

        blobStore.close();

        return snapshotInfo;
    }

    public SnapshotMetadataProvider(SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    public String getReason() {
        return snapshotInfo.reason();
    }

    public List<String> getIndices() {
        return snapshotInfo.indices();
    }

    public int getShardsFailed() {
        return snapshotInfo.failedShards();
    }

    public int getShardsSuccessful() {
        return snapshotInfo.successfulShards();
    }

    public int getShardsTotal() {
        return snapshotInfo.totalShards();
    }

    public String getState() {
        return snapshotInfo.state().toString();
    }
    
}
