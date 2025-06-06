package org.voltdb.xdcrutil.converters;

import org.voltdb.stream.api.Consumer;
import org.voltdb.stream.api.ExecutionContext;
import org.voltdb.stream.api.pipeline.VoltStreamFunction;
import org.voltdb.xdcrutil.XdcrConflictMessage;

public final class XDCRConflictMessageToStringConverter implements VoltStreamFunction<XdcrConflictMessage, String> {
    @Override
    public void process(XdcrConflictMessage input, Consumer<String> consumer, ExecutionContext context) {

        consumer.consume(input.getConflictPK() + " " + input.getOldJsonEncodedTuple() + "->"
                + input.getJsonEncodedTuple());

    }
}