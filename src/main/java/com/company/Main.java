package com.company;

import com.amazonaws.AmazonClientException;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Main {
    private static final String QUEUE_NAME = "MyArmaQueue";
    private static final String SOURCE_BUCKET_NAME = "stage1.alex6511.com";
    private static final String DST_BUCKET_NAME = "completed.alex6511.com";
    private static final String ERR_BUCKET_NAME = "error.alex6511.com";
    private static final String UNZIP_FILE_PATH = "C:\\Users\\Administrator\\Desktop\\unzipTo";
    private static final int BUFFER_SIZE = 4096;
    private static final String ARMA_TOOLS_LOCATION = "C:\\Users\\Administrator\\Desktop\\Arma Tools\\armake_v0.5.1\\MakeMission.Bat";
    private static final String PBO_MISSION_LOCATION = "D:\\A3Master\\mpmissions\\";
    private static final String PATH_TO_ARMA = "D:\\A3Master\\Arma3server_x64.exe";
    private static final String MODS = "@CBA_A3_BW;@ace_BW;@acre2;@BourbonMapRotation;@BourbonMods;@CUP_Terrains_Complete;" +
            "@Helvantis;@hlcmods;@K_MNP;@mbg;@miscMods;@Podagorsk;@potato;@RHSAFRF;@RHSGREF;@RHSUSAF;@sthud;@ToraBora;" +
            "@CUP_Weapons;@CUP_Units;@CUP_Vehicles;";


    private static final String PATH_TO_CONFIG = "D:\\A3Master\\server.cfg";
    private static final String PATH_TO_RPT = "C:\\Users\\Administrator\\AppData\\Local\\Arma 3";

    public static void main(String args[]) throws InterruptedException, ExecutionException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File("C:\\Users\\Administrator\\Desktop\\aws_sdk.properties")));

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
                    withMaxNumberOfMessages(1));
            List<Message> msgs = future.get().getMessages();
            if (msgs.size() == 0) {
                continue;
            }

            String missionName = "ERROR NO MISSION";
            String zipName = "ERROR NO ZIP";
            for (Message msg : msgs) {
                String handle = msg.getReceiptHandle();
                System.out.println(msg.getBody());


                int keyStart = msg.getBody().indexOf("key") + 6;
                int keyEnd = msg.getBody().indexOf("size") - 3;
                zipName = msg.getBody().substring(keyStart, keyEnd);

                System.out.println("The zip name was: " + zipName);
                String outputDirectory = downloadZip(zipName, s3Client);
                missionName = makePbo(zipName, outputDirectory);
                sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(url).withReceiptHandle(handle)); // delete message last in case something doesn't work
            }

            s3Client.deleteObject(SOURCE_BUCKET_NAME, zipName);

            boolean success = testOnArmaServer(missionName);
            if (success) {
                System.out.println("File tested successfully");
                File file = new File(PBO_MISSION_LOCATION + missionName);
                s3Client.putObject(DST_BUCKET_NAME, zipName, file);
            } else {
                try {
                    System.out.println("Testing Failed");
                    File file = new File(PBO_MISSION_LOCATION + missionName);
                    s3Client.putObject(ERR_BUCKET_NAME, zipName, file);
                } catch (AmazonClientException e) {
                    File temp = File.createTempFile(String.format("%d", zipName), ".tmp");
                    Formatter out = new Formatter(temp);
                    out.format("%s\n", e);
                    out.close();
                    s3Client.putObject(ERR_BUCKET_NAME, zipName, temp);
                    temp.delete();
                    e.printStackTrace();
                }
            }

        }
        System.out.println("All missions have been tested");
        ProcessBuilder processBuilder = new ProcessBuilder("shutdown", "/s");
        processBuilder.start();
    }

    private static boolean testOnArmaServer(String missionName) throws IOException {
        Boolean missionPassed = false;
        Path path = Paths.get(PATH_TO_CONFIG);
        Charset charset = StandardCharsets.UTF_8;

        String content = new String(Files.readAllBytes(path), charset);
        content = content.replaceAll("REPLACE_ME", missionName.substring(0, missionName.length() - 4));
        Files.write(path, content.getBytes(charset));


        System.out.println("Now Launching Arma server to test mission: " + missionName);
        Process process = new ProcessBuilder(PATH_TO_ARMA, "-config=server.cfg", "-autoinit", "-mod=expansion;heli;jets;kart;mark;orange;tank;" + MODS).start();
        try {
            Thread.sleep(180000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        process.destroy();
        File parseFile = lastFileModified(PATH_TO_RPT);
        BufferedReader reader = new BufferedReader(new FileReader(parseFile));
        String line;
        Set<String> results = new TreeSet<>();
        while ((line = reader.readLine()) != null) {
            results.add(line);

        }
        for (String result : results) {
            if (result.contains("CallExtension loaded: ace_advanced_ballistics")) {
                missionPassed = true;
            }
        }
        System.out.println("The mission: " + missionPassed);
        content = content.replaceAll(missionName.substring(0, missionName.length() - 4), "REPLACE_ME");
        Files.write(path, content.getBytes(charset));
        return missionPassed;
    }

    private static File lastFileModified(String dir) {
        File fl = new File(dir);
        File[] files = fl.listFiles(File::isFile);
        long lastMod = Long.MIN_VALUE;
        File choice = null;
        for (File file : files) {
            if (file.lastModified() > lastMod) {
                choice = file;
                lastMod = file.lastModified();
            }
        }
        return choice;
    }

    private static String makePbo(String zipName, String inputDirectory) throws IOException {
        Process process = new ProcessBuilder(ARMA_TOOLS_LOCATION, inputDirectory, PBO_MISSION_LOCATION + zipName.substring(0, zipName.length() - 4) + ".pbo").start();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        process.destroy();
        return zipName.substring(0, zipName.length() - 4) + ".pbo";
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
        File cfg = new File(destDirectory + File.separator + "cfg");
        File Loadouts = new File(destDirectory + File.separator + "Loadouts");
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        if (!cfg.exists()) {
            cfg.mkdir();
        }
        if (!Loadouts.exists()) {
            Loadouts.mkdir();
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

