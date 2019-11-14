package edu.stanford.futuredata.uniserve.awstest;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
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
    public void basicAWSTest() throws InterruptedException {
        int numServers = 10;

        AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest().withImageId("ami-0d5d9d301c853a04a")
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(numServers)
                .withMaxCount(numServers)
                .withKeyName("kraftp")
                .withSecurityGroups("kraftp-uniserve");

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

}
