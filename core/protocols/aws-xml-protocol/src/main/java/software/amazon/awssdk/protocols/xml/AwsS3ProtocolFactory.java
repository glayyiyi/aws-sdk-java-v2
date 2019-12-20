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

package software.amazon.awssdk.protocols.xml;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import software.amazon.awssdk.annotations.SdkProtectedApi;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.http.HttpResponseHandler;
import software.amazon.awssdk.protocols.query.unmarshall.XmlElement;
import software.amazon.awssdk.protocols.xml.internal.unmarshall.AwsXmlPredicatedResponseHandler;
import software.amazon.awssdk.protocols.xml.internal.unmarshall.AwsXmlUnmarshallingContext;

/**
 * Factory to generate the various protocol handlers and generators to be used for communicating with
 * Amazon S3. S3 has some unique differences from typical REST/XML that warrant a custom protocol factory.
 */
@SdkProtectedApi
public final class AwsS3ProtocolFactory extends AwsXmlProtocolFactory {
    private static final String ERROR_IN_SUCCESS_BODY_ELEMENT_NAME = "Error";

    private AwsS3ProtocolFactory(Builder builder) {
        super(builder);
    }

    /**
     * For Amazon S3, the Code, Message, and modeled fields are in the top level document.
     *
     * @param document Root XML document.
     * @return If error root is found than a fulfilled {@link Optional}, otherwise an empty one.
     */
    @Override
    Optional<XmlElement> getErrorRoot(XmlElement document) {
        return Optional.of(document);
    }

    private Optional<XmlElement> getErrorRootFromSuccessBody(XmlElement document) {
        return ERROR_IN_SUCCESS_BODY_ELEMENT_NAME.equals(document.elementName())
            ? Optional.of(document)
            : Optional.empty();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link AwsS3ProtocolFactory}.
     */
    public static final class Builder extends AwsXmlProtocolFactory.Builder<Builder> {

        private Builder() {
        }

        public AwsS3ProtocolFactory build() {
            return new AwsS3ProtocolFactory(this);
        }
    }

    @Override
    public <T extends AwsResponse> HttpResponseHandler<Response<T>> createCombinedResponseHandler(
        Supplier<SdkPojo> pojoSupplier, XmlOperationMetadata staxOperationMetadata) {

        return createErrorCouldBeInBodyResponseHandler(pojoSupplier, staxOperationMetadata);
    }

    private <T extends AwsResponse> HttpResponseHandler<Response<T>> createErrorCouldBeInBodyResponseHandler(
        Supplier<SdkPojo> pojoSupplier, XmlOperationMetadata staxOperationMetadata) {

        return new AwsXmlPredicatedResponseHandler<>(r -> pojoSupplier.get(),
                                                     createResponseTransformer(pojoSupplier),
                                                     createErrorTransformer(),
                                                     findErrorInS3Response(),
                                                     staxOperationMetadata.isHasStreamingSuccessResponse());
    }

    private Function<AwsXmlUnmarshallingContext, AwsXmlUnmarshallingContext> findErrorInS3Response() {
        return context -> {
            Optional<XmlElement> parsedRootXml = Optional.ofNullable(context.parsedRootXml());

            if (!context.sdkHttpFullResponse().isSuccessful()) {
                // Request was non-2xx, error is expected to be in root
                Optional<XmlElement> parsedErrorXml = parsedRootXml.flatMap(this::getErrorRoot);
                return context.toBuilder().responseIsSuccess(false).parsedErrorXml(parsedErrorXml.orElse(null)).build();
            }

            // Check body to see if an error turned up there
            Optional<XmlElement> parsedErrorXml = parsedRootXml.isPresent() ?
                getErrorRootFromSuccessBody(context.parsedRootXml()) : Optional.empty();

            // Request had an HTTP success code, but an error was found in the body
            return parsedErrorXml.map(xmlElement -> context.toBuilder()
                                                           .responseIsSuccess(false)
                                                           .parsedErrorXml(xmlElement)
                                                           .build())
                                 // Otherwise the response can be considered successful
                                 .orElseGet(() -> context.toBuilder()
                                                         .responseIsSuccess(true)
                                                         .build());

        };
    }
}
