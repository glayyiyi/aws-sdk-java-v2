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

package software.amazon.awssdk.services.rds;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getAllServeEvents;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.notMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.Header;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.util.List;

public class RdsPresignRequestTest {

    @Rule
    public WireMockRule mockServer = new WireMockRule(0);

    private final Region clientRegion = Region.US_WEST_2;
    private final String COMMON_BODY_PREFIX = "Action=CopyDBSnapshot&Version=2014-10-31";

    private RdsClient rdsClient;
    private RdsAsyncClient rdsAsyncClient;


    @Before
    public void setup() {

        rdsClient = RdsClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "skid")))
                .region(clientRegion)
                .endpointOverride(URI.create("http://localhost:" + mockServer.port()))
                .build();

        rdsAsyncClient = RdsAsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "skid")))
                .region(Region.US_WEST_2)
                .endpointOverride(URI.create("http://localhost:" + mockServer.port()))
                .build();
    }

    @Test
    public void callPresignEnabledMethod_withoutPresignUrl_withoutRegion_shouldNotPresign() {
        stubSimpleResponse();

        rdsClient.copyDBSnapshot(b -> b.copyTags(true));

        RequestPatternBuilder requestBodyPattern = postRequestedFor(anyUrl());

        verifyCommonQueryProtcolParams(1, requestBodyPattern);
        verify(requestBodyPattern.withRequestBody(notMatching("PreSignedUrl")));
    }

    @Test
    public void callPresignEnabledMethod_withPresignUrl_withoutRegion_shouldNotPresign() {
        stubSimpleResponse();

        rdsClient.copyDBSnapshot(b -> b.copyTags(true).preSignedUrl("PRESIGNED"));

        RequestPatternBuilder requestBodyPattern = postRequestedFor(anyUrl());

        verifyCommonQueryProtcolParams(1, requestBodyPattern);
        verify(requestBodyPattern.withRequestBody(containing("PreSignedUrl")));
    }

    @Test
    public void callPresignEnabledMethod_withoutPresignUrl_withRegion_shouldPresign() {
        stubSimpleResponse();

        rdsClient.copyDBSnapshot(b -> b.copyTags(true).sourceRegion("eu-central-1"));

        RequestPatternBuilder requestBodyPattern = postRequestedFor(anyUrl());

        verifyCommonQueryProtcolParams(1, requestBodyPattern);
        verify(requestBodyPattern
                       .withRequestBody(containing("PreSignedUrl"))
                       .withRequestBody(containing("DestinationRegion"))
                       .withRequestBody(notMatching("SourceRegion")));
    }

    @Test
    public void subsequentRequestsShouldNotKeepState() {
        stubSimpleResponse();

        rdsClient.copyDBSnapshot(b -> b.copyTags(true).sourceRegion("eu-central-1"));
        rdsClient.copyDBSnapshot(b -> b.kmsKeyId("myKms").sourceRegion("eu-central-1"));

        RequestPatternBuilder requestBodyPattern = postRequestedFor(anyUrl());
        verifyCommonQueryProtcolParams(2, requestBodyPattern);

        List<ServeEvent> allRequests = getAllServeEvents();

        String firstRequestBody = allRequests.get(1).getRequest().getBodyAsString();
        assertThat(firstRequestBody).contains("CopyTags%3Dtrue");

        String secondRequestBody = allRequests.get(0).getRequest().getBodyAsString();
        assertThat(secondRequestBody).contains("KmsKeyId%3DmyKms");
        assertThat(secondRequestBody).doesNotContain("CopyTags%3Dtrue");
    }

    @Test
    public void asyncCallPresignEnabledMethod_withoutPresignUrl_withoutRegion_shouldNotPresign() {
        stubMetadataResponse();

        rdsAsyncClient.copyDBSnapshot(b -> b.copyTags(true)).join();

        RequestPatternBuilder requestBodyPattern = postRequestedFor(anyUrl());

        verifyCommonQueryProtcolParams(1, requestBodyPattern);
        verify(requestBodyPattern.withRequestBody(notMatching("PreSignedUrl")));
    }

    @Test
    public void asyncCallPresignEnabledMethod_withPresignUrl_withoutRegion_shouldNotPresign() {
        stubSimpleResponse();

        rdsAsyncClient.copyDBSnapshot(b -> b.copyTags(true).preSignedUrl("PRESIGNED")).join();

        RequestPatternBuilder requestBodyPattern = postRequestedFor(anyUrl());

        verifyCommonQueryProtcolParams(1, requestBodyPattern);
        verify(requestBodyPattern.withRequestBody(containing("PreSignedUrl")));
    }

    @Test
    public void asyncCallPresignEnabledMethod_withoutPresignUrl_withRegion_shouldPresign() {
        stubSimpleResponse();

        rdsAsyncClient.copyDBSnapshot(b -> b.copyTags(true).sourceRegion("eu-central-1")).join();

        RequestPatternBuilder requestBodyPattern = postRequestedFor(anyUrl());

        verifyCommonQueryProtcolParams(1, requestBodyPattern);
        verify(requestBodyPattern
                       .withRequestBody(containing("PreSignedUrl"))
                       .withRequestBody(containing("DestinationRegion"))
                       .withRequestBody(notMatching("SourceRegion")));
    }

    private void stubSimpleResponse() {
        stubFor(post(anyUrl()).willReturn(aResponse()
                                                  .withStatus(200)
                                                  .withBody("<CopyDBSnapshotResponse/>")));
    }

    private void stubMetadataResponse() {
        stubFor(post(anyUrl()).willReturn(aResponse()
                                                  .withStatus(200)
                                                  .withBody("<CopyDBSnapshotResponse>"
                        + "<ResponseMetadata>"
                        + "<RequestId>"
                        + "REQUEST_ID"
                        + "</RequestId>"
                        + "</ResponseMetadata>"
                        + "</CopyDBSnapshotResponse>"))
        );
    }

    private void verifyCommonQueryProtcolParams(int times, RequestPatternBuilder requestPatternBuilder) {

        verify(times, requestPatternBuilder
                       .withHeader(Header.CONTENT_TYPE, equalTo("application/x-www-form-urlencoded; charset=UTF-8"))
                       .withHeader(Header.CONTENT_LENGTH, matching("\\d+"))
                       .withUrl("/")
                       .withRequestBody(containing(COMMON_BODY_PREFIX)));

    }

}
