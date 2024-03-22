package com.rfs.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.rfs.source_es_6_8.ShardMetadata;

/*
 * Taken from Elasticsearch 6.8, combining the SlicedInputStream and PartSliceStream classes
 * See: https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/index/snapshots/blobstore/SlicedInputStream.java
 * See: https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/repositories/blobstore/BlobStoreRepository.java#L1403
 */

public class PartSliceStream extends InputStream {
    private long slice = 0;
    private InputStream currentStream;
    private final long numSlices;
    private boolean initialized = false;
    private final Path shardDirPath;
    private final ShardMetadata.FileMetadata fileMetadata;

    public PartSliceStream(Path shardDirPath, ShardMetadata.FileMetadata fileMetadata) {
        this.numSlices = fileMetadata.getNumberOfParts();
        this.fileMetadata = fileMetadata;
        this.shardDirPath = shardDirPath;
    }

    protected InputStream openSlice(long slice) throws IOException {
        String sliceFilePath = shardDirPath + "/" + fileMetadata.partName(slice);
        Path path = Path.of(sliceFilePath);
        return Files.newInputStream(path);
    }

    private InputStream nextStream() throws IOException {
        assert initialized == false || currentStream != null;
        initialized = true;

        if (currentStream != null) {
            currentStream.close();
        }
        if (slice < numSlices) {
            currentStream = openSlice(slice++);
        } else {
            currentStream = null;
        }
        return currentStream;
    }

    private InputStream currentStream() throws IOException {
        if (currentStream == null) {
            return initialized ? null : nextStream();
        }
        return currentStream;
    }

    @Override
    public final int read() throws IOException {
        InputStream stream = currentStream();
        if (stream == null) {
            return -1;
        }
        final int read = stream.read();
        if (read == -1) {
            nextStream();
            return read();
        }
        return read;
    }

    @Override
    public final int read(byte[] buffer, int offset, int length) throws IOException {
        final InputStream stream = currentStream();
        if (stream == null) {
            return -1;
        }
        final int read = stream.read(buffer, offset, length);
        if (read <= 0) {
            nextStream();
            return read(buffer, offset, length);
        }
        return read;
    }

    @Override
    public final void close() throws IOException {
        if (currentStream != null) {
            currentStream.close();
        }
        initialized = true;
        currentStream = null;
    }

    @Override
    public final int available() throws IOException {
        InputStream stream = currentStream();
        return stream == null ? 0 : stream.available();
    }

}
