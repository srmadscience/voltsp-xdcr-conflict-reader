package org.voltdb.xdcrutil.filters;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.stream.api.Consumer;
import org.voltdb.stream.api.ExecutionContext;
import org.voltdb.stream.api.pipeline.VoltStreamFunction;
import org.voltdb.xdcrutil.XdcrActionType;
import org.voltdb.xdcrutil.XdcrConflictMessage;

public final class XDCRTableNameFilter implements VoltStreamFunction<XdcrConflictMessage, XdcrConflictMessage> {

    List<String> desiredTables = new ArrayList<String>();

    public XDCRTableNameFilter(String desiredTable) {
        super();
        desiredTables.add(desiredTable);
    }

    public XDCRTableNameFilter(String[] desiredTableArray) {
        super();
        if (desiredTableArray != null) {
            for (int i=0; i < desiredTableArray.length;i++) {
                desiredTables.add(desiredTableArray[i]);
            }
        }
    
    }

    @Override
    public void process(XdcrConflictMessage input, Consumer<XdcrConflictMessage> consumer, ExecutionContext context) {

        if (desiredTables.contains(input.getTableName())) {
            consumer.consume(input);
        }

    }

}