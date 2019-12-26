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

package software.amazon.awssdk.protocols.query.interceptor;

import static java.util.Collections.singletonList;
import static software.amazon.awssdk.utils.StringUtils.lowerCase;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

/**
 * Modifies an HTTP request by moving query parameters to the body under the following conditions:
 * - It is a POST request
 * - There is no content stream provider
 * - There are query parameters to transfer
 *
 * Observe that this interceptor should only be applied to the Query Protocol.
 */
@SdkInternalApi
public final class QueryParametersToBodyInterceptor implements ExecutionInterceptor {

    @Override
    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context,
                                                      ExecutionAttributes executionAttributes) {

        SdkHttpRequest httpRequest = context.httpRequest();

        if (!(httpRequest instanceof SdkHttpFullRequest)) {
            return httpRequest;
        }

        SdkHttpFullRequest httpFullRequest = (SdkHttpFullRequest) httpRequest;
        if (shouldPutParamsInBody(httpFullRequest)) {
            return changeQueryParametersToFormData(httpFullRequest);
        }
        return httpFullRequest;
    }

    private boolean shouldPutParamsInBody(SdkHttpFullRequest input) {
        return input.method() == SdkHttpMethod.POST &&
                !input.contentStreamProvider().isPresent() &&
                !CollectionUtils.isNullOrEmpty(input.rawQueryParameters());
    }

    private static SdkHttpRequest changeQueryParametersToFormData(SdkHttpFullRequest input) {
        byte[] params = SdkHttpUtils.encodeAndFlattenFormData(input.rawQueryParameters()).orElse("")
                .getBytes(StandardCharsets.UTF_8);

        return input.toBuilder().clearQueryParameters()
                .contentStreamProvider(() -> new ByteArrayInputStream(params))
                .putHeader("Content-Length", singletonList(String.valueOf(params.length)))
                .putHeader("Content-Type", singletonList("application/x-www-form-urlencoded; charset=" +
                                                                 lowerCase(StandardCharsets.UTF_8.toString())))
                .build();
    }

}
