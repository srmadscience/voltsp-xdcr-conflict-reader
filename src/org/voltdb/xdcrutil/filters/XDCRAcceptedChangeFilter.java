package org.voltdb.xdcrutil.filters;

import org.voltdb.stream.api.Consumer;
import org.voltdb.stream.api.ExecutionContext;
import org.voltdb.stream.api.pipeline.VoltStreamFunction;
import org.voltdb.xdcrutil.XdcrConflictMessage;

public final class XDCRAcceptedChangeFilter
        implements VoltStreamFunction<XdcrConflictMessage, XdcrConflictMessage> {

    boolean accepted;

    public XDCRAcceptedChangeFilter(boolean accepted) {
        super();
        this.accepted = accepted;
    }

    
    @Override
    public void process(XdcrConflictMessage input, Consumer<XdcrConflictMessage> consumer,
            ExecutionContext context) {

        if ( accepted == input.wasAccepted()) {
            consumer.consume(input);
        }

    }

}