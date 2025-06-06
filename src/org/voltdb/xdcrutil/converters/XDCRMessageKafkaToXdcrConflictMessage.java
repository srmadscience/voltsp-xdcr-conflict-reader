package org.voltdb.xdcrutil.converters;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.voltdb.stream.api.Consumer;
import org.voltdb.stream.api.ExecutionContext;
import org.voltdb.stream.api.kafka.KafkaRequest;
import org.voltdb.stream.api.pipeline.VoltStreamFunction;
import org.voltdb.xdcrutil.XdcrConflictMessage;
import org.voltdb.xdcrutil.XdcrFormatException;

public final class XDCRMessageKafkaToXdcrConflictMessage
        implements VoltStreamFunction<KafkaRequest<ByteBuffer, ByteBuffer>, XdcrConflictMessage> {

    @Override
    public void process(KafkaRequest<ByteBuffer, ByteBuffer> input, Consumer<XdcrConflictMessage> consumer,
            ExecutionContext context) {

        String k = StandardCharsets.UTF_8.decode(input.getKey()).toString();
        String p = StandardCharsets.UTF_8.decode(input.getValue()).toString();

        XdcrConflictMessage m = null;
        try {
            m = new XdcrConflictMessage(k, p);

            consumer.consume(m);

        } catch (XdcrFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}