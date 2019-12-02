package edu.stanford.futuredata.uniserve.awstest;

import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.invoke.LambdaFunction;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;


class AWSLambdaFunctionInput {
    private String keyName;
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }
    public String getKeyName() {
        return this.keyName;
    }
}


interface HelloWorldService {
    @LambdaFunction(functionName="HelloWorld")
    long helloWorld(AWSLambdaFunctionInput input);
}

public class AWSLambdaTests {
    private static final Logger logger = LoggerFactory.getLogger(AWSLambdaTests.class);

    public long uniserveTestLambda(AWSLambdaFunctionInput input) throws InterruptedException, IOException {
        long startTime = System.currentTimeMillis();
        Path downloadPath = Path.of("/tmp/bob.txt");
        logger.info("Start download! {}", System.currentTimeMillis() - startTime);
        TransferManager tx = TransferManagerBuilder.standard().build();
        Download d = tx.download("kraftp-uniserve", input.getKeyName(), downloadPath.toFile());
        d.waitForCompletion();
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

//    @Test
    public void testRunLambda() {
        logger.info("Start Lambda Test");
        final HelloWorldService s = LambdaInvokerFactory.builder().lambdaClient(AWSLambdaClientBuilder.defaultClient()).build(HelloWorldService.class);
        AWSLambdaFunctionInput i = new AWSLambdaFunctionInput();
        long startTime = System.currentTimeMillis();
        i.setKeyName("bob-10M.txt");
        long len = s.helloWorld(i);
        logger.info("Elapsed Time: {} ms", System.currentTimeMillis() - startTime);
        logger.info("Length: {}", len);

    }

}
