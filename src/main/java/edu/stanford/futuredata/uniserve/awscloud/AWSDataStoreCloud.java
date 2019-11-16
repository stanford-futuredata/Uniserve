package edu.stanford.futuredata.uniserve.awscloud;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.*;
import edu.stanford.futuredata.uniserve.datastore.DataStoreCloud;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

public class AWSDataStoreCloud implements DataStoreCloud {
    private static final Logger logger = LoggerFactory.getLogger(AWSDataStoreCloud.class);

    private final String bucket;

    public AWSDataStoreCloud(String bucket) {
        // TODO:  Currently assuming bucket already exists.
        this.bucket = bucket;
    }

    @Override
    public Optional<String> uploadShardToCloud(Path shardDirectory, String shardName) {
        TransferManager tx = TransferManagerBuilder.standard().build();
        File dirFile = shardDirectory.toFile();
        try {
            MultipleFileUpload mfu = tx.uploadDirectory(bucket, shardName, dirFile, true);
            for (Upload upload : mfu.getSubTransfers()) {
                upload.waitForUploadResult();
            }
        } catch (AmazonServiceException | InterruptedException e) {
            logger.warn("Shard upload failed: {}", e.getMessage());
            return Optional.empty();
        }
        return Optional.of(shardName);
    }


    @Override
    public int downloadShardFromCloud(Path shardDirectory, String shardCloudName) {
        TransferManager tx = TransferManagerBuilder.standard().build();
        File dirFile = shardDirectory.toFile();
        try {
            MultipleFileDownload mfd = tx.downloadDirectory(bucket, shardCloudName, dirFile);
            mfd.waitForCompletion();
        } catch (AmazonServiceException | InterruptedException e) {
            logger.warn("Shard download failed: {}", e.getMessage());
            return 1;
        }
        return 0;
    }
}
