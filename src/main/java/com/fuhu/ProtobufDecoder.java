package com.fuhu;

/**
 * Created by yizhao on 6/24/15.
 */
import org.apache.log4j.Logger;

import com.fuhu.kafka.proto.KafkaProto.KafkaLoggingMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class ProtobufDecoder  implements Decoder<Object> {
    private static final Logger LOGGER = Logger.getLogger(ProtobufDecoder.class);
    public ProtobufDecoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }

    @Override
    public Object fromBytes(byte[] bytes) {
        try {
            if (bytes == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("At ProtobufDecoder class, bytes is null");
                }
                return null;
            }
            return KafkaLoggingMessage.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}