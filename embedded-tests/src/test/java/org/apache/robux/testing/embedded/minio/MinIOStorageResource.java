/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.robux.testing.embedded.minio;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.robux.common.aws.AWSModule;
import org.apache.robux.storage.s3.S3StorageRobuxModule;
import org.apache.robux.testing.embedded.EmbeddedRobuxCluster;
import org.apache.robux.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.MinIOContainer;

/**
 * A MinIO container resource for use in embedded tests as deep storage.
 * Sets up MinIO as S3-compatible storage and configures Robux's S3 connector.
 */
public class MinIOStorageResource extends TestcontainerResource<MinIOContainer>
{
  private static final String MINIO_IMAGE = "minio/minio:latest";
  private static final String DEFAULT_BUCKET = "robux-deep-storage";
  private static final String DEFAULT_BASE_KEY = "robux/segments";
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";

  private final String bucket;
  private final String baseKey;
  private AmazonS3 s3Client;

  public MinIOStorageResource()
  {
    this(DEFAULT_BUCKET, DEFAULT_BASE_KEY);
  }

  public MinIOStorageResource(String bucket, String baseKey)
  {
    this.bucket = bucket;
    this.baseKey = baseKey;
  }

  @Override
  protected MinIOContainer createContainer()
  {
    return new MinIOContainer(MINIO_IMAGE)
        .withUserName(getAccessKey())
        .withPassword(getSecretKey());
  }

  @Override
  public void onStarted(EmbeddedRobuxCluster cluster)
  {
    s3Client = createS3Client();
    s3Client.createBucket(bucket);

    cluster.addExtension(S3StorageRobuxModule.class);
    cluster.addExtension(AWSModule.class);

    // Configure storage bucket and base key
    cluster.addCommonProperty("robux.storage.type", "s3");
    cluster.addCommonProperty("robux.storage.bucket", getBucket());
    cluster.addCommonProperty("robux.storage.baseKey", getBaseKey());

    // Configure indexer logs
    cluster.addCommonProperty("robux.indexer.logs.type", "s3");
    cluster.addCommonProperty("robux.indexer.logs.s3Bucket", getBucket());
    cluster.addCommonProperty("robux.indexer.logs.s3Prefix", "robux/indexing-logs");

    // Configure S3 connection properties
    cluster.addCommonProperty("robux.s3.endpoint.url", getEndpointUrl());
    cluster.addCommonProperty("robux.s3.accessKey", getAccessKey());
    cluster.addCommonProperty("robux.s3.secretKey", getSecretKey());
    cluster.addCommonProperty("robux.s3.enablePathStyleAccess", "true");
    cluster.addCommonProperty("robux.s3.protocol", "http");
  }

  public String getBucket()
  {
    return bucket;
  }

  public String getBaseKey()
  {
    return baseKey;
  }

  public String getAccessKey()
  {
    return ACCESS_KEY;
  }

  public String getSecretKey()
  {
    return SECRET_KEY;
  }

  public String getEndpointUrl()
  {
    ensureRunning();
    return getContainer().getS3URL();
  }

  public AmazonS3 getS3Client()
  {
    ensureRunning();
    return s3Client;
  }

  private AmazonS3 createS3Client()
  {
    return AmazonS3Client
        .builder()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(getEndpointUrl(), "us-east-1"))
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretKey())))
        .withPathStyleAccessEnabled(true)
        .build();
  }
}
