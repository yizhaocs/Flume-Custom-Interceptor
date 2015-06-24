package com.fuhu;

/**
 * Created by yizhao on 6/24/15.
 */
import org.apache.log4j.Logger;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import com.fuhu.social.proto.KafkaProto.KafkaLoggingMessage;

public class ProtobufEncoder implements Encoder<Object> {
    private static final Logger LOGGER = Logger
            .getLogger(ProtobufEncoder.class);

    public ProtobufEncoder(VerifiableProperties verifiableProperties) {
		/* This constructor must be present for successful compile. */
    }

    @Override
    public byte[] toBytes(Object object) {
        if (object == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("At ProtobufEncoder class, object is null");
            }
            return null;
        }

        return ((KafkaLoggingMessage) object).toByteArray();
    }
}