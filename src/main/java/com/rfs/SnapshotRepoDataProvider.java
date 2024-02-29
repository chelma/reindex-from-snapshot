package com.rfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SnapshotRepoDataProvider {
    private final SnapshotRepoData repoData;

    public SnapshotRepoDataProvider(String filePath) throws IOException{
        this.repoData = SnapshotRepoData.fromRepoFile(filePath);
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
            targetSnapshot.indexMetadataLookup.keySet().forEach(indexId -> {
                repoData.indices.forEach((indexName, rawIndex) -> {
                    if (indexId.equals(rawIndex.id)) {
                        matchedIndices.add(SnapshotRepoData.Index.fromRawIndex(indexName, rawIndex));
                    }
                });
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

    public String getIndexMetadataId (String snapshotName, String indexName) {
        String indexId = getIndexId(indexName);
        if (indexId == null) {
            return null;
        }

        String metadataLookupKey = repoData.snapshots.stream()
                .filter(snapshot -> snapshot.name.equals(snapshotName))
                .map(snapshot -> snapshot.indexMetadataLookup.get(indexId))
                .findFirst()
                .orElse(null);
        if (metadataLookupKey == null) {
            return null;
        }

        return repoData.indexMetadataIdentifiers.get(metadataLookupKey);
    }
}
