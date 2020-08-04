package dev.bigspark.lb.ibmmq;

import _ss_com.streamsets.pipeline.support.service.ServiceErrors;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.Stage.Context;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.RecoverableDataParserException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class ServicesUtil {
    public ServicesUtil() {
    }

    public static List<Record> parseAll(Context stageContext, ToErrorContext toErrorContext, boolean produceSingleRecordPerMessage, String messageId, byte[] payload) throws StageException {
        ArrayList records = new ArrayList();

        Record record;
        try {
            DataParser parser = ((DataFormatParserService)stageContext.getService(DataFormatParserService.class)).getParser(messageId, payload);
            Throwable var25 = null;

            try {
                record = null;

                boolean recoverableExceptionHit;
                do {
                    try {
                        recoverableExceptionHit = false;
                        record = parser.parse();
                    } catch (RecoverableDataParserException var20) {
                        handleException(stageContext, toErrorContext, messageId, var20, var20.getUnparsedRecord());
                        recoverableExceptionHit = true;
                        continue;
                    }

                    if (record != null) {
                        records.add(record);
                    }
                } while(record != null || recoverableExceptionHit);
            } catch (Throwable var21) {
                var25 = var21;
                throw var21;
            } finally {
                if (parser != null) {
                    if (var25 != null) {
                        try {
                            parser.close();
                        } catch (Throwable var19) {
                            var25.addSuppressed(var19);
                        }
                    } else {
                        parser.close();
                    }
                }

            }
        } catch (DataParserException | IOException var23) {
            record = stageContext.createRecord(messageId);
            record.set(Field.create(payload));
            handleException(stageContext, toErrorContext, messageId, var23, record);
            return records;
        }

        if (produceSingleRecordPerMessage) {
            List<Field> list = new ArrayList();
            Iterator var26 = records.iterator();

            while(var26.hasNext()) {
                record = (Record)var26.next();
                list.add(record.get());
            }

            record = (Record)records.get(0);
            record.set(Field.create(list));
            records.clear();
            records.add(record);
        }

        return records;
    }

    private static void handleException(Context stageContext, ToErrorContext errorContext, String messageId, Exception ex, Record record) throws StageException {
        switch(stageContext.getOnErrorRecord()) {
            case TO_ERROR:
                errorContext.toError(record, ServiceErrors.SERVICE_ERROR_001, new Object[]{messageId, ex.toString(), ex});
            case DISCARD:
                return;
            case STOP_PIPELINE:
                if (ex instanceof StageException) {
                    throw (StageException)ex;
                }

                throw new StageException(ServiceErrors.SERVICE_ERROR_001, new Object[]{messageId, ex.toString(), ex});
            default:
                throw new IllegalStateException(Utils.format("Unknown on error value '{}'", new Object[]{stageContext, ex}));
        }
    }
}
