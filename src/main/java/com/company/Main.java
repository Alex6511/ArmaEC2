package com.company;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;
import com.amazonaws.services.sqs.model.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Main {
    private static final String QUEUE_NAME = "MyArmaQueue";
    private static final String SOURCE_BUCKET_NAME = "stage1.alex6511.com";
    private static final String DST_BUCKET_NAME = "completed.alex6511.com";
    private static final String UNZIP_FILE_PATH = "C:\\Users\\Alex6\\Dropbox\\User Folders\\NEW HORIZONS\\Desktop\\unzipTo";
    private static final int BUFFER_SIZE = 4096;
    private static final String ARMA_TOOLS_LOCATION = "C:\\Users\\Alex6\\Dropbox\\User Folders\\NEW HORIZONS\\Desktop\\Arma Tools\\armake_v0.5.1\\armake_w64.exe";
    private static final String PBO_MISSION_LOCATION = "C:\\Users\\Alex6\\Dropbox\\User Folders\\NEW HORIZONS\\Desktop\\pboMission\\";


    public static void main(String args[]) throws InterruptedException, ExecutionException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File("aws_sdk.properties")));

        AWSCredentials credentials = new BasicAWSCredentials(properties.getProperty("aws_access_key_id"),
                properties.getProperty("aws_secret_access_key"));

        AmazonSQSAsyncClient asynClient = new AmazonSQSAsyncClient(credentials);

        QueueBufferConfig config = new QueueBufferConfig()
                .withLongPoll(true).withLongPollWaitTimeoutSeconds(20);

        AmazonSQSAsync sqs = new AmazonSQSBufferedAsyncClient(asynClient, config);

        AmazonS3 s3Client = new AmazonS3Client(credentials);

        if (!s3Client.doesBucketExist(SOURCE_BUCKET_NAME)) {
            System.err.printf("Bucket %s does not exist", SOURCE_BUCKET_NAME);
            System.exit(1);
        }

        String url = "";
        try {
            url = sqs.getQueueUrl(new GetQueueUrlRequest(QUEUE_NAME)).getQueueUrl();
        } catch (QueueDoesNotExistException e) {
            System.err.printf("Queue %s does not exist", QUEUE_NAME);
            System.exit(1);
        }


        System.out.println("Receiving messages from MyQueue.\n");

        while (true) {
            GetQueueAttributesResult result = sqs.getQueueAttributes(
                    new GetQueueAttributesRequest(url, Arrays.asList("ApproximateNumberOfMessages")));
            if (Integer.parseInt(result.getAttributes().get("ApproximateNumberOfMessages")) == 0) {
                System.out.println("Queue appears to be empty");
                break;
            }
            Future<ReceiveMessageResult> future = sqs.receiveMessageAsync(new ReceiveMessageRequest(url).
                    withMaxNumberOfMessages(20));
            List<Message> msgs = future.get().getMessages();
            if (msgs.size() == 0) {
                continue;
            }

            File temp = File.createTempFile(String.format("%d", 100 + (new Random()).nextInt(10000)), ".tmp");
            Formatter out = new Formatter(temp);
            for (Message msg : msgs) {
                String handle = msg.getReceiptHandle();
                System.out.println(msg.getBody());
                out.format("%s\n", msg.getBody());


                String zipName;
                int keyStart = msg.getBody().indexOf("key") + 6;
                int keyEnd = msg.getBody().indexOf("size") - 3;
                zipName = msg.getBody().substring(keyStart, keyEnd);

                System.out.println("The zip name was: " + zipName);
                String outputDirectory = downloadZip(zipName, s3Client);
                makePbo(zipName, outputDirectory);

                sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(url).withReceiptHandle(handle)); // delete message last in case something doesn't work
            }
            out.close();

//            s3Client.putObject(DST_BUCKET_NAME, , temp); // TODO UPLOAD TO FINAL
            System.out.println("file uploaded to s3");
            temp.delete();
//                Thread.sleep(10000);
        }
    }

    private static void makePbo(String zipName, String inputDirectory) throws IOException {
        Process process = new ProcessBuilder(ARMA_TOOLS_LOCATION, "build", "-f", inputDirectory, PBO_MISSION_LOCATION + zipName.substring(0, zipName.length() - 4) + ".pbo").start();
    }

    private static String downloadZip(String zipName, AmazonS3 s3Client) throws IOException {
        // Download the zip from S3 into a stream
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(SOURCE_BUCKET_NAME, zipName));
        ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent());

        return unzip(zis, UNZIP_FILE_PATH, zipName);
    }

    private static String unzip(ZipInputStream zis, String destDirectory, String zipName) throws IOException {
        destDirectory = destDirectory + File.separator + zipName.substring(0, zipName.length() - 4);
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        ZipEntry entry = zis.getNextEntry();
        // iterates over entries in the zip file
        while (entry != null) {
            String filePath = destDirectory + File.separator + entry.getName();
            if (!entry.isDirectory()) {
                // if the entry is a file, extracts it
                extractFile(zis, filePath);
            } else {
                // if the entry is a directory, make the directory
                File dir = new File(filePath);
                dir.mkdir();
            }
            zis.closeEntry();
            entry = zis.getNextEntry();
        }
        zis.close();
        return destDirectory;
    }

    private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }
}

