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
    private final VoltXdcrConflictReaderPipeline voltXdcrConflictReader;

    /**
     * @param voltXdcrConflictReader
     */
    XDCRMessageProcessor(VoltXdcrConflictReaderPipeline voltXdcrConflictReader) {
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

    @Override
    public void initialize(Consumer<XdcrConflictMessage> consumer) {
        // TODO Auto-generated method stub
        VoltStreamFunction.super.initialize(consumer);
        VoltXdcrConflictReaderPipeline.msg("initialize called");
    }

    @Override
    public void nextBatchStarts(long batchId) {
        // TODO Auto-generated method stub
        VoltStreamFunction.super.nextBatchStarts(batchId);
        VoltXdcrConflictReaderPipeline.msg("starting batch " + batchId);
          }

    @Override
    public void batchProcessed(long batchId) {
        // TODO Auto-generated method stub
        VoltStreamFunction.super.batchProcessed(batchId);
        VoltXdcrConflictReaderPipeline.msg("ending batch " + batchId);
    }

    @Override
    public Type getType() {
        // TODO Auto-generated method stub
        VoltXdcrConflictReaderPipeline.msg("called getType");
        return VoltStreamFunction.super.getType();
        
    }
}