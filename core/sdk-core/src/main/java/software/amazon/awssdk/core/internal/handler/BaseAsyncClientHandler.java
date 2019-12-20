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

package software.amazon.awssdk.core.internal.handler;

import static software.amazon.awssdk.utils.FunctionalUtils.runAndLogError;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.handler.AsyncClientHandler;
import software.amazon.awssdk.core.client.handler.ClientExecutionParams;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.http.Crc32Validation;
import software.amazon.awssdk.core.http.ExecutionContext;
import software.amazon.awssdk.core.http.HttpResponseHandler;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;
import software.amazon.awssdk.core.internal.http.AmazonAsyncHttpClient;
import software.amazon.awssdk.core.internal.http.IdempotentAsyncResponseHandler;
import software.amazon.awssdk.core.internal.http.TransformingAsyncResponseHandler;
import software.amazon.awssdk.core.internal.http.async.AsyncAfterTransmissionInterceptorCallingResponseHandler;
import software.amazon.awssdk.core.internal.http.async.AsyncResponseHandler;
import software.amazon.awssdk.core.internal.http.async.AsyncStreamingResponseHandler;
import software.amazon.awssdk.core.internal.http.async.CombinedResponseAsyncHttpResponseHandler;
import software.amazon.awssdk.core.internal.util.ThrowableUtils;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.utils.CompletableFutureUtils;
import software.amazon.awssdk.utils.Logger;

@SdkInternalApi
public abstract class BaseAsyncClientHandler extends BaseClientHandler implements AsyncClientHandler {
    private static final Logger log = Logger.loggerFor(BaseAsyncClientHandler.class);
    private final SdkClientConfiguration clientConfiguration;
    private final AmazonAsyncHttpClient client;
    private final Function<SdkHttpFullResponse, SdkHttpFullResponse> crc32Validator;

    protected BaseAsyncClientHandler(SdkClientConfiguration clientConfiguration,
                                     AmazonAsyncHttpClient client) {
        super(clientConfiguration);
        this.clientConfiguration = clientConfiguration;
        this.client = client;
        this.crc32Validator = response -> Crc32Validation.validate(isCalculateCrc32FromCompressedData(), response);
    }

    @Override
    public <InputT extends SdkRequest, OutputT extends SdkResponse> CompletableFuture<OutputT> execute(
        ClientExecutionParams<InputT, OutputT> executionParams) {

        validateExecutionParams(executionParams);
        ExecutionContext executionContext = createExecutionContext(executionParams, createInitialExecutionAttributes());
        TransformingAsyncResponseHandler<Response<OutputT>> combinedResponseHandler;

        if (executionParams.getCombinedResponseHandler() == null) {
            // All other protocols take this path
            HttpResponseHandler<OutputT> decoratedResponseHandlers =
                decorateResponseHandlers(executionParams.getResponseHandler(), executionContext);

            TransformingAsyncResponseHandler<OutputT> successHandler =
                new AsyncResponseHandler<>(decoratedResponseHandlers,
                                           crc32Validator,
                                           executionContext.executionAttributes());

            TransformingAsyncResponseHandler<? extends SdkException> errorHandler =
                resolveErrorResponseHandler(executionParams, executionContext, crc32Validator);
            combinedResponseHandler = new CombinedResponseAsyncHttpResponseHandler<>(successHandler, errorHandler);
        } else {
            // XML non-streaming protocol takes this path
            HttpResponseHandler<Response<OutputT>> decoratedResponseHandlers =
                decorateSuccessResponseHandlers(executionParams.getCombinedResponseHandler(), executionContext);

            combinedResponseHandler = new AsyncResponseHandler<>(decoratedResponseHandlers,
                                                                 crc32Validator,
                                                                 executionContext.executionAttributes());
        }

        return doExecute(executionParams, executionContext, combinedResponseHandler);
    }

    @Override
    public <InputT extends SdkRequest, OutputT extends SdkResponse, ReturnT> CompletableFuture<ReturnT> execute(
        ClientExecutionParams<InputT, OutputT> executionParams,
        AsyncResponseTransformer<OutputT, ReturnT> asyncResponseTransformer) {

        validateExecutionParams(executionParams);

        if (executionParams.getCombinedResponseHandler() != null) {
            // There is no support for catching errors in a body for streaming responses. Our codegen must never
            // attempt to do this.
            throw new IllegalArgumentException("A streaming 'asyncResponseTransformer' may not be used when a "
                                               + "'combinedResponseHandler' has been specified in a "
                                               + "ClientExecutionParams object.");
        }

        ExecutionAttributes executionAttributes = createInitialExecutionAttributes();

        AsyncStreamingResponseHandler<OutputT, ReturnT> asyncStreamingResponseHandler =
            new AsyncStreamingResponseHandler<>(asyncResponseTransformer);

        // For streaming requests, prepare() should be called as early as possible to avoid NPE in client
        // See https://github.com/aws/aws-sdk-java-v2/issues/1268. We do this with a wrapper that caches the prepare
        // result until the execution attempt number changes. This guarantees that prepare is only called once per
        // execution.
        TransformingAsyncResponseHandler<ReturnT> wrappedAsyncStreamingResponseHandler =
            IdempotentAsyncResponseHandler.create(
                asyncStreamingResponseHandler,
                () -> executionAttributes.getAttribute(SdkInternalExecutionAttribute.EXECUTION_ATTEMPT),
                Integer::equals);
        wrappedAsyncStreamingResponseHandler.prepare();

        ExecutionContext context = createExecutionContext(executionParams, executionAttributes);

        HttpResponseHandler<OutputT> decoratedResponseHandlers =
            decorateResponseHandlers(executionParams.getResponseHandler(), context);

        asyncStreamingResponseHandler.responseHandler(decoratedResponseHandlers);

        TransformingAsyncResponseHandler<? extends SdkException> errorHandler =
            resolveErrorResponseHandler(executionParams, context, crc32Validator);

        TransformingAsyncResponseHandler<Response<ReturnT>> combinedResponseHandler =
            new CombinedResponseAsyncHttpResponseHandler<>(wrappedAsyncStreamingResponseHandler, errorHandler);

        return doExecute(executionParams, context, combinedResponseHandler);
    }

    private <InputT extends SdkRequest, OutputT extends SdkResponse, ReturnT> CompletableFuture<ReturnT> doExecute(
        ClientExecutionParams<InputT, OutputT> executionParams,
        ExecutionContext executionContext,
        TransformingAsyncResponseHandler<Response<ReturnT>> asyncResponseHandler) {

        try {

            // Running beforeExecution interceptors and modifyRequest interceptors.
            InterceptorContext finalizeSdkRequestContext = finalizeSdkRequest(executionContext);
            InputT inputT = (InputT) finalizeSdkRequestContext.request();

            // Running beforeMarshalling, afterMarshalling and modifyHttpRequest, modifyHttpContent,
            // modifyAsyncHttpContent interceptors
            InterceptorContext finalizeSdkHttpRequestContext = finalizeSdkHttpFullRequest(executionParams,
                                                                                          executionContext,
                                                                                          inputT,
                                                                                          clientConfiguration);

            SdkHttpFullRequest marshalled = (SdkHttpFullRequest) finalizeSdkHttpRequestContext.httpRequest();

            // For non-streaming requests, RequestBody can be modified in the interceptors. eg:
            // CreateMultipartUploadRequestInterceptor
            if (!finalizeSdkHttpRequestContext.asyncRequestBody().isPresent() &&
                finalizeSdkHttpRequestContext.requestBody().isPresent()) {
                marshalled = marshalled.toBuilder()
                                       .contentStreamProvider(
                                           finalizeSdkHttpRequestContext.requestBody()
                                                                        // To appease static-code analysis
                                                                        .orElse(null)
                                                                        .contentStreamProvider())
                                       .build();
            }

            CompletableFuture<ReturnT> invokeFuture =
                invoke(marshalled,
                       finalizeSdkHttpRequestContext.asyncRequestBody().orElse(null),
                       inputT,
                       executionContext,
                       new AsyncAfterTransmissionInterceptorCallingResponseHandler<>(asyncResponseHandler,
                                                                                     executionContext));

            CompletableFuture<ReturnT> exceptionTranslatedFuture = invokeFuture.handle((resp, err) -> {
                if (err != null) {
                    throw ThrowableUtils.failure(err);
                }
                return resp;
            });

            return CompletableFutureUtils.forwardExceptionTo(exceptionTranslatedFuture, invokeFuture);
        } catch (Throwable t) {
            runAndLogError(
                log.logger(),
                "Error thrown from TransformingAsyncResponseHandler#onError, ignoring.",
                // TODO: Have the StandardAsyncHttpResponseHandler forward errors and create a new wrapper in the
                // MakeAsyncHttpRequestStage to stop errors from retries escaping. Then remove this hack.
                () -> asyncResponseHandler.onError(t));
            return CompletableFutureUtils.failedFuture(ThrowableUtils.asSdkException(t));
        }
    }

    @Override
    public void close() {
        client.close();
    }

    /**
     * Error responses are never streaming so we always use {@link AsyncResponseHandler}.
     *
     * @return Async handler for error responses.
     */
    private TransformingAsyncResponseHandler<? extends SdkException> resolveErrorResponseHandler(
        ClientExecutionParams<?, ?> executionParams,
        ExecutionContext executionContext,
        Function<SdkHttpFullResponse, SdkHttpFullResponse> responseAdapter) {
        return new AsyncResponseHandler<>(executionParams.getErrorResponseHandler(),
                                          responseAdapter,
                                          executionContext.executionAttributes());
    }

    /**
     * Invoke the request using the http client. Assumes credentials (or lack thereof) have been
     * configured in the ExecutionContext beforehand.
     **/
    private <InputT extends SdkRequest, OutputT> CompletableFuture<OutputT> invoke(
        SdkHttpFullRequest request,
        AsyncRequestBody requestProvider,
        InputT originalRequest,
        ExecutionContext executionContext,
        TransformingAsyncResponseHandler<Response<OutputT>> responseHandler) {
        return client.requestExecutionBuilder()
                     .requestProvider(requestProvider)
                     .request(request)
                     .originalRequest(originalRequest)
                     .executionContext(executionContext)
                     .execute(responseHandler);
    }
}
