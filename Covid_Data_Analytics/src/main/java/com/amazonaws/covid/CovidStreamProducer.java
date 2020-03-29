package com.amazonaws.covid;
/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.covid.model.CovidData;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CovidStreamProducer {

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (C:\\Users\\SHOBHITAGRAWAL\\.aws\\credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    private static AmazonKinesis kinesis;
    
    private static Gson gson = new GsonBuilder().create();
    
    private static final String APPLICATION_STREAM_NAME = "Covid-Stream";

    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\SHOBHITAGRAWAL\\.aws\\credentials).
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\SHOBHITAGRAWAL\\.aws\\credentials), and is in valid format.",
                    e);
        }

        kinesis = AmazonKinesisClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("us-east-1")
            .build();
    }
    
   public static void connectToAWS() throws Exception {
	   init();
   }
   
   public static void createStream() throws Exception {
	   
	   final Integer myStreamSize = 1;

       // Describe the stream and check if it exists.
       DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(APPLICATION_STREAM_NAME);
       try {
           StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
           System.out.printf("Stream %s has a status of %s.\n", APPLICATION_STREAM_NAME, streamDescription.getStreamStatus());

           if ("DELETING".equals(streamDescription.getStreamStatus())) {
               System.out.println("Stream is being deleted. This sample will now exit.");
               System.exit(0);
           }

           // Wait for the stream to become active if it is not yet ACTIVE.
           if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
               waitForStreamToBecomeAvailable(APPLICATION_STREAM_NAME);
           }
       } catch (ResourceNotFoundException ex) {
           System.out.printf("Stream %s does not exist. Creating it now.\n", APPLICATION_STREAM_NAME);

           // Create a stream. The number of shards determines the provisioned throughput.
           CreateStreamRequest createStreamRequest = new CreateStreamRequest();
           createStreamRequest.setStreamName(APPLICATION_STREAM_NAME);
           createStreamRequest.setShardCount(myStreamSize);
           kinesis.createStream(createStreamRequest);
           // The stream is now being created. Wait for it to become active.
           waitForStreamToBecomeAvailable(APPLICATION_STREAM_NAME);
       }

       // List all of my streams.
       ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
       listStreamsRequest.setLimit(10);
       ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
       List<String> streamNames = listStreamsResult.getStreamNames();
       while (listStreamsResult.isHasMoreStreams()) {
           if (streamNames.size() > 0) {
               listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
           }

           listStreamsResult = kinesis.listStreams(listStreamsRequest);
           streamNames.addAll(listStreamsResult.getStreamNames());
       }
       // Print all of my streams.
       System.out.println("List of my streams: ");
       for (int i = 0; i < streamNames.size(); i++) {
           System.out.println("\t- " + streamNames.get(i));
       }
	   
   }
   
   public static void putDataToStream(CovidData data) {
	   
	   System.out.printf("Putting records in stream : %s until this application is stopped...\n", APPLICATION_STREAM_NAME);
       System.out.println("Press CTRL-C to stop.");
       // Write records to the stream until this program is aborted.
       //while (true) {
       //long createTime = System.currentTimeMillis();
       PutRecordRequest putRecordRequest = new PutRecordRequest();
       putRecordRequest.setStreamName(APPLICATION_STREAM_NAME);
       String json = gson.toJson(data);
       System.out.println("Put record data-->"+json);
       putRecordRequest.setData(ByteBuffer.wrap(String.format(json).getBytes()));
       putRecordRequest.setPartitionKey(String.format(data.getCountry()));
       PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
       System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                putRecordRequest.getPartitionKey(),
                putRecordResult.getShardId(),
                putRecordResult.getSequenceNumber());
   }
   
   public static void putDataToStream(String data) {
	   
	   System.out.printf("Putting records in stream : %s until this application is stopped...\n", APPLICATION_STREAM_NAME);
       System.out.println("Press CTRL-C to stop.");
       // Write records to the stream until this program is aborted.
       //while (true) {
       //long createTime = System.currentTimeMillis();
       PutRecordRequest putRecordRequest = new PutRecordRequest();
       putRecordRequest.setStreamName(APPLICATION_STREAM_NAME);
       System.out.println("Put record data-->"+data);
       putRecordRequest.setData(ByteBuffer.wrap(String.format(data).getBytes()));
       putRecordRequest.setPartitionKey(String.format("123"));
       PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
       System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                putRecordRequest.getPartitionKey(),
                putRecordResult.getShardId(),
                putRecordResult.getSequenceNumber());
   }

    private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
    }
    
}
