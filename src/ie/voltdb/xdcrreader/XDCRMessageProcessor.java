package ie.voltdb.xdcrreader;

import org.voltdb.stream.api.Consumer;
import org.voltdb.stream.api.ExecutionContext;
import org.voltdb.stream.api.pipeline.VoltStreamFunction;
import org.voltdb.xdcrutil.XdcrConflictMessage;
import org.voltdb.xdcrutil.XdcrRecordCache;

final class XDCRMessageProcessor implements VoltStreamFunction<XdcrConflictMessage, XdcrConflictMessage> {

    /**
     * 
     */
    private final VoltXdcrConflictReaderWrangler voltXdcrConflictReader;

    /**
     * @param voltXdcrConflictReader
     */
    XDCRMessageProcessor(VoltXdcrConflictReaderWrangler voltXdcrConflictReader) {
        this.voltXdcrConflictReader = voltXdcrConflictReader;
    }

    @Override
    public void process(XdcrConflictMessage input, Consumer<XdcrConflictMessage> consumer,
            ExecutionContext context) {

        XdcrRecordCache c = XdcrRecordCache.getInstance();

        input.setPk(this.voltXdcrConflictReader.pkMap);
        c.put(input);

        String basePK = input.getConflictPK().substring(0, input.getConflictPK().lastIndexOf('\t'));

        XdcrConflictMessage newMessage = c.get(basePK + "\tNEW");
        XdcrConflictMessage extMessage = c.get(basePK + "\tEXT");
        XdcrConflictMessage expMessage = c.get(basePK + "\tEXP");

        if (newMessage != null && extMessage != null && expMessage != null) {
            newMessage.setOldJsonEncodedTuple(extMessage.getJsonEncodedTuple());
            consumer.consume(newMessage);
            c.remove(basePK + "\tNEW");
            c.remove(basePK + "\tEXT");
            c.remove(basePK + "\tEXP");

        }

    }
}