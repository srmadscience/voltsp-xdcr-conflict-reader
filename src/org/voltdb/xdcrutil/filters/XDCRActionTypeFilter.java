package org.voltdb.xdcrutil.filters;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.stream.api.Consumer;
import org.voltdb.stream.api.ExecutionContext;
import org.voltdb.stream.api.pipeline.VoltStreamFunction;
import org.voltdb.xdcrutil.XdcrActionType;
import org.voltdb.xdcrutil.XdcrConflictMessage;

public final class XDCRActionTypeFilter implements VoltStreamFunction<XdcrConflictMessage, XdcrConflictMessage> {

    List<XdcrActionType> desiredTypes = new ArrayList<XdcrActionType>();

    public XDCRActionTypeFilter(XdcrActionType desiredType) {
        super();
        desiredTypes.add(desiredType);
    }

    public XDCRActionTypeFilter(XdcrActionType[] desiredTypeArray) {
        super();
        if (desiredTypeArray != null) {
            for (int i=0; i < desiredTypeArray.length;i++) {
                desiredTypes.add(desiredTypeArray[i]);
            }
        }
    
    }

    @Override
    public void process(XdcrConflictMessage input, Consumer<XdcrConflictMessage> consumer, ExecutionContext context) {

        if (desiredTypes.contains(input.getActionType())) {
            consumer.consume(input);
        }

    }

}