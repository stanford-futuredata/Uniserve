package edu.stanford.futuredata.uniserve.awstest;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.util.Base64;
import org.jboss.netty.handler.codec.base64.Base64Encoder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AWSTests {

    private static final Logger logger = LoggerFactory.getLogger(AWSTests.class);

    //@Test // Don't run this test by default, it's expensive.
    public void launchAndTerminateInstances() throws InterruptedException {
        int numServers = 10;

        AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

        String scriptText = "#!/bin/bash\ntouch /home/ubuntu/bob.bob\nchmod +666 /home/ubuntu/bob.bob\n";
        String encoded = Base64.encodeAsString(scriptText.getBytes());

        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest().withImageId("ami-0d5d9d301c853a04a") // Default Ubuntu disk
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(numServers)
                .withMaxCount(numServers)
                .withKeyName("kraftp")
                .withSecurityGroups("kraftp-uniserve")
                .withUserData(encoded);

        long startTime = System.currentTimeMillis();
        RunInstancesResult result = ec2.runInstances(
                runInstancesRequest);

        assertNotNull(result);
        assertNotNull(result.getReservation());
        assertNotNull(result.getReservation().getInstances());
        assertEquals(numServers, result.getReservation().getInstances().size());

        List<String> instanceIdsList = new ArrayList<>();
        for(int i = 0; i < numServers; i++) {
            instanceIdsList.add(result.getReservation().getInstances().get(i).getInstanceId());
        }

        List<Thread> threads = new ArrayList<>();
        for(int i = 0; i < numServers; i++) {
            int finalI = i;
            Thread t = new Thread(() -> {
                String instanceId = instanceIdsList.get(finalI);
                int stateCode = 0;
                while (stateCode != 16) { // Loop until the instance is in the "running" state.
                    DescribeInstancesRequest describeInstanceRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
                    DescribeInstancesResult describeInstanceResult = ec2.describeInstances(describeInstanceRequest);
                    InstanceState instanceState = describeInstanceResult.getReservations().get(0).getInstances().get(0).getState();
                    stateCode = instanceState.getCode();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {}
                }
                long endTime = System.currentTimeMillis();
                logger.info("Start-up Time: {}", (endTime - startTime) / 1000);
            });
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {
            t.join();
        }

        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest().withInstanceIds(instanceIdsList);
        TerminateInstancesResult terminateInstancesResult = ec2.terminateInstances(terminateInstancesRequest);
        assertNotNull(terminateInstancesResult);
        assertEquals(numServers, terminateInstancesResult.getTerminatingInstances().size());
    }

    //@Test // Don't run this test by default, it's expensive.
    public void launchDataStore() throws InterruptedException {
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

        String scriptText = "#!/bin/bash\ncd /home/ubuntu/Uniserve\nnohup java -jar target/Uniserve-1.0-SNAPSHOT-fat-tests.jar -datastore -zh 172.31.28.212 -zp 2181 -h aws -p 8000 > /home/ubuntu/datastore.log &\n chmod +444 /home/ubuntu/datastore.log\n";
        String encoded = Base64.encodeAsString(scriptText.getBytes());

        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest().withImageId("ami-029f2656fd1c7df8b") // Uniserve datastore image
                        .withInstanceType(InstanceType.T2Micro)
                        .withMinCount(1)
                        .withMaxCount(1)
                        .withKeyName("kraftp")
                        .withSecurityGroups("kraftp-uniserve")
                        .withUserData(encoded);

        long startTime = System.currentTimeMillis();
        RunInstancesResult result = ec2.runInstances(
                runInstancesRequest);

        assertNotNull(result);
        assertNotNull(result.getReservation());
        assertNotNull(result.getReservation().getInstances());
        assertEquals(1, result.getReservation().getInstances().size());

        String instanceId = result.getReservation().getInstances().get(0).getInstanceId();
        int stateCode = 0;
        while (stateCode != 16) { // Loop until the instance is in the "running" state.
            DescribeInstancesRequest describeInstanceRequest = new DescribeInstancesRequest().withInstanceIds(instanceId);
            DescribeInstancesResult describeInstanceResult = ec2.describeInstances(describeInstanceRequest);
            InstanceState instanceState = describeInstanceResult.getReservations().get(0).getInstances().get(0).getState();
            stateCode = instanceState.getCode();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
        }
        long endTime = System.currentTimeMillis();
        logger.info("Start-up Time: {}", (endTime - startTime) / 1000);

        Thread.sleep(30000); // Let the startup script complete.

        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest().withInstanceIds(instanceId);
        TerminateInstancesResult terminateInstancesResult = ec2.terminateInstances(terminateInstancesRequest);
        assertNotNull(terminateInstancesResult);
        assertEquals(1, terminateInstancesResult.getTerminatingInstances().size());
    }

}
