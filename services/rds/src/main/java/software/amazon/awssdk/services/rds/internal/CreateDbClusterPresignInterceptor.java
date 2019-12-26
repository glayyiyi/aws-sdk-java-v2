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

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.RdsRequest;
import software.amazon.awssdk.services.rds.transform.CreateDbClusterRequestMarshaller;

/**
 * Handler for pre-signing {@link CreateDbClusterRequest}.
 */
@SdkInternalApi
public final class CreateDbClusterPresignInterceptor extends RdsPresignInterceptor<CreateDbClusterRequest> {

    public CreateDbClusterPresignInterceptor() {
        super(CreateDbClusterRequest.class);
    }

    @Override
    protected PresignableRequest adaptRequest(final CreateDbClusterRequest originalRequest) {
        return new PresignableRequest() {

            @Override
            public String getPresignedUrl() {
                return originalRequest.preSignedUrl();
            }

            @Override
            public String getSourceRegion() {
                return originalRequest.sourceRegion();
            }

            @Override
            public SdkHttpFullRequest marshallQueryParams() {
                return getMarshaller(CreateDbClusterRequestMarshaller.SDK_OPERATION_BINDING)
                        .marshallQueryParams(originalRequest);
            }
        };
    }

    @Override
    protected RdsRequest modifyRequestForPresigning(final CreateDbClusterRequest originalRequest, String presignedUrl) {
        return originalRequest.toBuilder()
                .preSignedUrl(presignedUrl)
                .sourceRegion(null)
                .build();
    }

}
