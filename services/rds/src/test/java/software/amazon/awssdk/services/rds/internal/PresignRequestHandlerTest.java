/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.awssdk.services.rds.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.awscore.endpoint.DefaultServiceEndpointBuilder;
import software.amazon.awssdk.core.Protocol;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.model.CopyDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.RdsRequest;
import software.amazon.awssdk.services.rds.transform.CopyDbSnapshotRequestMarshaller;

/**
 * Unit Tests for {@link RdsPresignInterceptor}
 */
public class PresignRequestHandlerTest {

    private final AwsBasicCredentials CREDENTIALS = AwsBasicCredentials.create("foo", "bar");
    private final Region DESTINATION_REGION = Region.of("us-west-2");

    private RdsPresignInterceptor<CopyDbSnapshotRequest> presignInterceptor;

    @Before
    public void setup() {
        presignInterceptor = new CopyDbSnapshotPresignInterceptor();
    }

    @Test
    public void testSetsPresignedUrl() {
        CopyDbSnapshotRequest request = makeTestRequest();
        CopyDbSnapshotRequest presignedRequest = (CopyDbSnapshotRequest) modifySdkRequest(presignInterceptor, request);

        assertNotNull(presignedRequest.preSignedUrl());
    }

    @Test
    public void testComputesPresignedUrlCorrectly() {
        final CopyDbSnapshotRequest request = CopyDbSnapshotRequest.builder()
                .sourceDBSnapshotIdentifier("arn:aws:rds:us-east-1:123456789012:snapshot:rds:test-instance-ss-2016-12-20-23-19")
                .targetDBSnapshotIdentifier("test-instance-ss-copy-2")
                .sourceRegion("us-east-1")
                .kmsKeyId("arn:aws:kms:us-west-2:123456789012:key/11111111-2222-3333-4444-555555555555")
                .build();

        Calendar c = new GregorianCalendar();
        c.setTimeZone(TimeZone.getTimeZone("UTC"));
        c.set(2016, Calendar.DECEMBER, 21, 18, 7, 35);

        Clock signingDateOverride = Mockito.mock(Clock.class);
        when(signingDateOverride.millis()).thenReturn(c.getTimeInMillis());

        RdsPresignInterceptor<CopyDbSnapshotRequest> interceptor = new CopyDbSnapshotPresignInterceptor(signingDateOverride);

        CopyDbSnapshotRequest presignedRequest = (CopyDbSnapshotRequest) modifySdkRequest(interceptor, request);

        final String expectedPreSignedUrl = "https://rds.us-east-1.amazonaws.com?" +
                "Action=CopyDBSnapshot" +
                "&Version=2014-10-31" +
                "&SourceDBSnapshotIdentifier=arn%3Aaws%3Ards%3Aus-east-1%3A123456789012%3Asnapshot%3Ards%3Atest-instance-ss-2016-12-20-23-19" +
                "&TargetDBSnapshotIdentifier=test-instance-ss-copy-2" +
                "&KmsKeyId=arn%3Aaws%3Akms%3Aus-west-2%3A123456789012%3Akey%2F11111111-2222-3333-4444-555555555555" +
                "&DestinationRegion=us-west-2" +
                "&X-Amz-Algorithm=AWS4-HMAC-SHA256" +
                "&X-Amz-Date=20161221T180735Z" +
                "&X-Amz-SignedHeaders=host" +
                "&X-Amz-Expires=604800" +
                "&X-Amz-Credential=foo%2F20161221%2Fus-east-1%2Frds%2Faws4_request" +
                "&X-Amz-Signature=f839ca3c728dc96e7c978befeac648296b9f778f6724073de4217173859d13d9";

        assertEquals(expectedPreSignedUrl, presignedRequest.preSignedUrl());
    }

    @Test
    public void testSkipsPresigningIfUrlSet() {
        CopyDbSnapshotRequest request = CopyDbSnapshotRequest.builder()
                .sourceRegion("us-west-2")
                .preSignedUrl("PRESIGNED")
                .build();

        CopyDbSnapshotRequest presignedRequest = (CopyDbSnapshotRequest) modifySdkRequest(presignInterceptor, request);

        assertEquals("PRESIGNED", presignedRequest.preSignedUrl());
    }

    @Test
    public void testSkipsPresigningIfSourceRegionNotSet() {
        CopyDbSnapshotRequest request = CopyDbSnapshotRequest.builder().build();

        CopyDbSnapshotRequest presignedRequest = (CopyDbSnapshotRequest) modifySdkRequest(presignInterceptor, request);

        assertNull(presignedRequest.preSignedUrl());
    }

    @Test
    public void testParsesDestinationRegionfromRequestEndpoint() throws URISyntaxException {
        CopyDbSnapshotRequest request = CopyDbSnapshotRequest.builder()
                .sourceRegion("us-east-1")
                .build();
        Region destination = Region.of("us-west-2");

        CopyDbSnapshotRequest presignedRequest = (CopyDbSnapshotRequest) modifySdkRequest(presignInterceptor, request);

        final URI presignedUrl = new URI(presignedRequest.preSignedUrl());
        assertTrue(presignedUrl.toString().contains("DestinationRegion=" + destination.id()));
    }

    @Test
    public void testSourceRegionRemovedFromOriginalRequest() {
        CopyDbSnapshotRequest request = makeTestRequest();
        CopyDbSnapshotRequest presignedRequest = (CopyDbSnapshotRequest) modifySdkRequest(presignInterceptor, request);

        assertNull(presignedRequest.sourceRegion());
    }

    private ExecutionAttributes executionAttributes() {
        return new ExecutionAttributes()
                .putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS, CREDENTIALS)
                .putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION, DESTINATION_REGION);
    }

    private CopyDbSnapshotRequest makeTestRequest() {
        return CopyDbSnapshotRequest.builder()
                .sourceDBSnapshotIdentifier("arn:aws:rds:us-east-1:123456789012:snapshot:rds:test-instance-ss-2016-12-20-23-19")
                .targetDBSnapshotIdentifier("test-instance-ss-copy-2")
                .sourceRegion("us-east-1")
                .kmsKeyId("arn:aws:kms:us-west-2:123456789012:key/11111111-2222-3333-4444-555555555555")
                .build();
    }

    private SdkRequest modifySdkRequest(ExecutionInterceptor interceptor, RdsRequest request) {
        InterceptorContext context = InterceptorContext.builder().request(request).build();
        return interceptor.modifyRequest(context, executionAttributes());
    }
}
