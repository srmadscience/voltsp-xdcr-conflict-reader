package org.voltdb.xdcrutil.filters;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.stream.api.Consumer;
import org.voltdb.stream.api.ExecutionContext;
import org.voltdb.stream.api.pipeline.VoltStreamFunction;
import org.voltdb.xdcrutil.*;

final class XDCRConflictTypeFilter implements VoltStreamFunction<XdcrConflictMessage, XdcrConflictMessage> {

    List<XdcrConflictType> conflictTypes = new ArrayList<XdcrConflictType>();

    public XDCRConflictTypeFilter(XdcrConflictType conflictType) {
        super();
        conflictTypes.add(conflictType);
    }

    public XDCRConflictTypeFilter(XdcrConflictType[] conflictTypeArray) {
        super();
        if (conflictTypeArray != null) {
            for (int i=0; i < conflictTypeArray.length;i++) {
                conflictTypes.add(conflictTypeArray[i]);
            }
        }
    
    }

    @Override
    public void process(XdcrConflictMessage input, Consumer<XdcrConflictMessage> consumer, ExecutionContext context) {

        if (conflictTypes.contains(input.getConflictType())) {
            consumer.consume(input);
        }

    }

}