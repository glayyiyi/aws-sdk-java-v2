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

import static software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute.AWS_CREDENTIALS;

import java.net.URI;
import java.time.Clock;
import java.util.Objects;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.awscore.endpoint.DefaultServiceEndpointBuilder;
import software.amazon.awssdk.core.Protocol;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.protocols.core.OperationInfo;
import software.amazon.awssdk.protocols.query.AwsQueryProtocolFactory;
import software.amazon.awssdk.protocols.query.internal.marshall.QueryProtocolMarshaller;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.model.RdsRequest;

/**
 * Abstract pre-sign handler that follows the pre-signing scheme outlined in the 'RDS Presigned URL for Cross-Region Copying'
 * SEP.
 *
 * @param <T> The request type.
 */
@SdkInternalApi
public abstract class RdsPresignInterceptor<T extends RdsRequest> implements ExecutionInterceptor {

    protected static final AwsQueryProtocolFactory PROTOCOL_FACTORY = AwsQueryProtocolFactory
        .builder()
        .clientConfiguration(SdkClientConfiguration.builder()
                                                   .option(SdkClientOption.ENDPOINT, URI.create("http://localhost"))
                                                   .build())
        .build();

    private static final String SERVICE_NAME = "rds";
    private static final String PARAM_SOURCE_REGION = "SourceRegion";
    private static final String PARAM_DESTINATION_REGION = "DestinationRegion";

    public interface PresignableRequest {

        String getPresignedUrl();

        String getSourceRegion();

        SdkHttpFullRequest marshallQueryParams();
    }

    private final Class<T> requestClassToPreSign;

    private final Clock signingOverrideClock;

    public RdsPresignInterceptor(Class<T> requestClassToPreSign) {
        this(requestClassToPreSign, null);
    }

    public RdsPresignInterceptor(Class<T> requestClassToPreSign, Clock signingOverrideClock) {
        this.requestClassToPreSign = requestClassToPreSign;
        this.signingOverrideClock = signingOverrideClock;
    }

    @Override
    public final SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes executionAttributes) {

        SdkRequest originalRequest = context.request();

        if (!requestClassToPreSign.isInstance(originalRequest)) {
            return originalRequest;
        }

        T rdsRequest = requestClassToPreSign.cast(context.request());
        PresignableRequest presignableRequest = adaptRequest(rdsRequest);

        if (Objects.nonNull(presignableRequest.getPresignedUrl())) {
            return originalRequest;
        }

        String sourceRegion = presignableRequest.getSourceRegion();
        if (Objects.isNull(sourceRegion)) {
            return originalRequest;
        }

        String presignedUrl = createPresignedUrl(presignableRequest, executionAttributes, sourceRegion);

        return modifyRequestForPresigning(rdsRequest, presignedUrl);
    }

    /**
     * Adapts the request to the {@link PresignableRequest}.
     *
     * @param originalRequest the original request
     * @return a PresignableRequest
     */
    protected abstract PresignableRequest adaptRequest(T originalRequest);

    /**
     * Modifies the original request.
     *
     * @param originalRequest the original request
     * @return a modified request
     */
    protected abstract SdkRequest modifyRequestForPresigning(T originalRequest, String presignedUrl);


    protected QueryProtocolMarshaller getMarshaller(OperationInfo operationInfo) {
        return PROTOCOL_FACTORY.createQueryProtocolMarshaller(operationInfo);
    }

    private String createPresignedUrl(PresignableRequest presignableRequest,
                                      ExecutionAttributes executionAttributes,
                                      String signingRegion) {


        SdkHttpFullRequest.Builder marshalledRequest = presignableRequest.marshallQueryParams()
                .toBuilder()
                .uri(createEndpoint(signingRegion));

        String destinationRegion = executionAttributes.getAttribute(AwsSignerExecutionAttribute.SIGNING_REGION).id();

        SdkHttpFullRequest requestToPresign = marshalledRequest
                .method(SdkHttpMethod.GET)
                .putRawQueryParameter(PARAM_DESTINATION_REGION, destinationRegion)
                .removeQueryParameter(PARAM_SOURCE_REGION)
                .build();

        Aws4Signer signer = Aws4Signer.create();
        Aws4PresignerParams presignerParams = Aws4PresignerParams.builder()
                                                     .signingRegion(Region.of(signingRegion))
                                                     .signingName(SERVICE_NAME)
                                                     .signingClockOverride(signingOverrideClock)
                                                     .awsCredentials(executionAttributes.getAttribute(AWS_CREDENTIALS))
                                                     .build();

        SdkHttpFullRequest presignedHttpRequest = signer.presign(requestToPresign, presignerParams);

        return presignedHttpRequest.getUri().toString();
    }

    private URI createEndpoint(String regionName) {
        Region region = Region.of(regionName);

        if (region == null) {
            throw SdkClientException.builder()
                                    .message("{" + SERVICE_NAME + ", " + regionName + "} was not "
                                            + "found in region metadata. Update to latest version of SDK and try again.")
                                    .build();
        }

        return new DefaultServiceEndpointBuilder(SERVICE_NAME, Protocol.HTTPS.toString())
                .withRegion(region)
                .getServiceEndpoint();
    }

}
