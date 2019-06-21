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

package software.amazon.awssdk.enhanced.dynamodb.converter.attribute.bundled;

import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.converter.attribute.ConversionContext;
import software.amazon.awssdk.enhanced.dynamodb.converter.string.bundled.BooleanStringConverter;
import software.amazon.awssdk.enhanced.dynamodb.model.ItemAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeConvertingVisitor;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;

/**
 * A converter between {@link AtomicBoolean} and {@link ItemAttributeValue}.
 *
 * <p>
 * This stores values in DynamoDB as a boolean.
 *
 * <p>
 * This supports reading every boolean value supported by DynamoDB, making it fully compatible with custom converters as well
 * as internal converters (e.g. {@link AtomicBooleanAttributeConverter}).
 *
 * <p>
 * This can be created via {@link #create()}.
 */
@SdkPublicApi
@ThreadSafe
@Immutable
public final class BooleanAttributeConverter implements AttributeConverter<Boolean> {
    private static final Visitor VISITOR = new Visitor();
    private static final BooleanStringConverter STRING_CONVERTER = BooleanStringConverter.create();

    private BooleanAttributeConverter() {}

    public static BooleanAttributeConverter create() {
        return new BooleanAttributeConverter();
    }

    @Override
    public TypeToken<Boolean> type() {
        return TypeToken.of(Boolean.class);
    }

    @Override
    public ItemAttributeValue toAttributeValue(Boolean input, ConversionContext context) {
        return ItemAttributeValue.fromBoolean(input);
    }

    @Override
    public Boolean fromAttributeValue(ItemAttributeValue input,
                                      ConversionContext context) {
        return input.convert(VISITOR);
    }

    private static final class Visitor extends TypeConvertingVisitor<Boolean> {
        private Visitor() {
            super(Boolean.class, BooleanAttributeConverter.class);
        }

        @Override
        public Boolean convertString(String value) {
            return STRING_CONVERTER.fromString(value);
        }

        @Override
        public Boolean convertBoolean(Boolean value) {
            return value;
        }
    }
}