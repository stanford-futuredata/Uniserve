package edu.stanford.futuredata.uniserve.awstest;

import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaFunction;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface.KVShardFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;


class S3LambdaTestInput {
    private String keyName;
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }
    public String getKeyName() {
        return this.keyName;
    }
}

class DataStoreLambdaTestInput {
    private String zkHost;
    private int zkPort;
    public void setZkHost(String zkHost) { this.zkHost = zkHost; }
    public void setZkPort(int zkPort) { this.zkPort = zkPort; }
    public String getZkHost() { return this.zkHost; }
    public int getZkPort() { return this.zkPort; }
}

interface s3LambdaTestService {
    @LambdaFunction(functionName="S3LambdaTest")
    long s3LambdaTest(S3LambdaTestInput input);
}

public class AWSLambdaTests {
    private static final Logger logger = LoggerFactory.getLogger(AWSLambdaTests.class);


    public long s3LambdaTest(S3LambdaTestInput input) throws InterruptedException, IOException {
        long startTime = System.currentTimeMillis();
        Path downloadPath = Path.of("/tmp/bob.txt");
        logger.info("Start function! {}", System.currentTimeMillis() - startTime);
        TransferManager tx = TransferManagerBuilder.standard().build();
        logger.info("Start download! {}", System.currentTimeMillis() - startTime);
        Download d = tx.download("kraftp-uniserve", input.getKeyName(), downloadPath.toFile());
        d.waitForCompletion();
//        FileUtils.copyURLToFile(new URL(" https://kraftp-uniserve.s3.us-east-2.amazonaws.com/bob-10M.txt "),
//                downloadPath.toFile(),
//                1000,
//                1000);
        logger.info("Finish download!  Size: {} bytes {}", downloadPath.toFile().length(), System.currentTimeMillis() - startTime);
        try {
            BufferedReader brTest = new BufferedReader(new FileReader(downloadPath.toFile()));
            String text = brTest.readLine();
            logger.info("First line: {} {}", text, System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            logger.error("File not found");
        }
        return downloadPath.toFile().length();
    }

    public void dataStoreLambdaTest(DataStoreLambdaTestInput input) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        DataStore<KVRow, KVShard> dataStore = new DataStore<>(new AWSDataStoreCloud("kraftp-uniserve"), new KVShardFactory(), Path.of("/var/tmp/KVUniserve"), input.getZkHost(), input.getZkPort(), "placeholder", 8000);
        logger.info("Start datastore! {}", System.currentTimeMillis() - startTime);
        dataStore.startServing();
        Thread.sleep(5000);
        logger.info("Stop datastore! {}", System.currentTimeMillis() - startTime);
        dataStore.shutDown();
    }

//    @Test // Expensive to run!
    public void testS3Lambda() {
        logger.info("Start Lambda Test");
        final s3LambdaTestService s = LambdaInvokerFactory.builder().lambdaClient(AWSLambdaClientBuilder.defaultClient()).build(s3LambdaTestService.class);
        S3LambdaTestInput i = new S3LambdaTestInput();
        long startTime = System.currentTimeMillis();
        i.setKeyName("bob-10M.txt");
        long len = s.s3LambdaTest(i);
        logger.info("Elapsed Time: {} ms", System.currentTimeMillis() - startTime);
        logger.info("Length: {}", len);

    }

}
