# Vert.x S3 client

A fully functional Vert.x client for S3

## Compatibility
- Java 8+
- Vert.x 3.x.x

## Dependencies

### Dependency Vert.x 3.3.x
### Maven
```xml
<dependency>
    <groupId>com.hubrick.vertx</groupId>
    <artifactId>vertx-s3-client</artifactId>
    <version>2.0.0</version>
</dependency>
```

## How to use
```java
        final S3ClientOptions clientOptions = new S3ClientOptions()
                .setAwsRegion("eu-central-1")
                .setAwsServiceName("s3")
                .setAwsAccessKey("AKIDEXAMPLE")
                .setAwsSecretKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");
                //.setSignPayload(true);

        final S3Client s3Client = new S3Client(vertx, clientOptions);

        s3Client.getObject(
                "bucket", 
                "key",
                new GetObjectRequest().withResponseContentType("application/json"),
                response -> System.out.println("Response from AWS: " + response.statusMessage()),
                Throwable::printStackTrace
        );

        s3Client.putObject(
                "bucket", 
                "key",
                new PutObjectRequest().withContentType("application/json"),
                Buffer.buffer("test"),
                response -> System.out.println("Response from AWS: " + response.statusMessage()),
                Throwable::printStackTrace
        );
        
        s3Client.deleteObject(
                "bucket", 
                "key",
                new DeleteObjectRequest(),
                response -> System.out.println("Response from AWS: " + response.statusMessage()),
                Throwable::printStackTrace
        );
        
        s3Client.copyObject(
                "sourceBucket", 
                "sourceKey",
                "destinationBucket", 
                "destinationKey",
                new CopyObjectRequest(),
                response -> System.out.println("Response from AWS: " + response.statusMessage()),
                Throwable::printStackTrace
        );
        
        // List bucket v2
        s3Client.getBucket(
                "bucket",
                new GetBucketRequest().withPrefix("prefix"),
                getBucketRespone -> System.out.println("Response from AWS: " + getBucketRespone.getName()),
                Throwable::printStackTrace
        );
```
 
## License
Apache License, Version 2.0

Contains the "AWS Signature Version 4 Test Suite" 
from http://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html
Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under Apache License, Version 2.0


