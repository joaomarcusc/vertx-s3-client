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
    <version>1.0.0</version>
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

        final S3Client s3Client = new S3Client(
                vertx,
                clientOptions);

        s3Client.put("bucket", "key",
                Buffer.buffer("test"),
                response -> System.out.println("Response from AWS: " + response.statusMessage()),
                Throwable::printStackTrace);

```
 
## License
Apache License, Version 2.0

Contains the "AWS Signature Version 4 Test Suite" 
from http://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html
Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under Apache License, Version 2.0


