package com.rfs;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SnapshotRepoDataProvider {
    private final SnapshotRepoData repoData;

    public SnapshotRepoDataProvider(Path dirPath) throws IOException{
        this.repoData = SnapshotRepoData.fromRepoDir(dirPath);
    }

    public Path getSnapshotDirPath() {
        return repoData.filePath.getParent();
    }

    public List<SnapshotRepoData.Index> getIndices() {
        return repoData.indices.entrySet().stream()
                .map(entry -> SnapshotRepoData.Index.fromRawIndex(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    public List<SnapshotRepoData.Index> getIndicesInSnapshot(String snapshotName) {
        List<SnapshotRepoData.Index> matchedIndices = new ArrayList<>();
        SnapshotRepoData.Snapshot targetSnapshot = repoData.snapshots.stream()
            .filter(snapshot -> snapshotName.equals(snapshot.name))
            .findFirst()
            .orElse(null);

        if (targetSnapshot != null) {
            repoData.indices.forEach((indexName, rawIndex) -> {
                if (rawIndex.snapshots.contains(targetSnapshot.uuid)) {
                    matchedIndices.add(SnapshotRepoData.Index.fromRawIndex(indexName, rawIndex));
                }
            });
        }
        return matchedIndices;
    }

    public List<SnapshotRepoData.Snapshot> getSnapshots() {
        return repoData.snapshots;
    }
    
    public String getSnapshotId(String snapshotName) {
        for (SnapshotRepoData.Snapshot snapshot : repoData.snapshots) {
            if (snapshot.name.equals(snapshotName)) {
                return snapshot.uuid;
            }
        }
        return null;
    }

    public String getIndexId(String indexName) {
        return repoData.indices.get(indexName).id;
    }
}
