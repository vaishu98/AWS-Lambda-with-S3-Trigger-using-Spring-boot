package com.appsdeveloperblog.s3lambda;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class S3EventHandler implements RequestHandler<S3Event, String> {

    private final String accessKeyId = System.getenv("accessKeyId");
    private final String secretAccessKey = System.getenv("secretAccessKey");
    private final String region = System.getenv("region");
    private final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    AmazonS3 s3client = AmazonS3ClientBuilder
            .standard()
            .withRegion(Regions.fromName(region))
            .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
            .build();
    static final Logger log = LoggerFactory.getLogger(S3EventHandler.class);

    @Override
    public String handleRequest(S3Event s3Event, Context context) {

        log.info("Lambda function is invoked: Processing the uploads........." + s3Event.toJson());

        String BucketName = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String FileName = s3Event.getRecords().get(0).getS3().getObject().getKey();

        log.info("File - "+ FileName+" uploaded into "+
                BucketName+" bucket at "+ s3Event.getRecords().get(0).getEventTime());
        try (InputStream is = s3client.getObject(BucketName, FileName).getObjectContent()) {
            log.info("File Contents : "+StreamUtils.copyToString(is, StandardCharsets.UTF_8));
        }catch (IOException e){
            e.printStackTrace();
            return "Error reading contents of the file";
        }
        return null;
    }
}
