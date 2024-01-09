package flink.debug.entity;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DebugSink extends RichSinkFunction<Row> implements CheckpointedFunction {

    protected ListState<Row> resultState;
    protected List<Row> localResults;
    protected int index = StreamSinkTest.Instance().newSinkId() ;
    protected Map<Integer,List<Row>> globalResults;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.resultState= context.getOperatorStateStore().getListState(new ListStateDescriptor<Row>("", Row.class));
        this.localResults = new Vector<>();
        if(context.isRestored()){
            Iterable<Row> rows = resultState.get();
            for(Row row:rows){
                this.localResults.add(row);
            }
        }

        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        StreamSinkTest.Instance().putSinkIndexAndGlobalResult(index,taskId,localResults);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.resultState.clear();
        resultState.addAll(localResults);

    }

    private void cleanAndStashGlobalResult(){
        if(null == globalResults){
            Map<Integer, List<Row>> removed = StreamSinkTest.Instance().removeSinkData(index);
            this.globalResults = removed;
        }
    }

    public List<Row> getResult(){
        cleanAndStashGlobalResult();
        LinkedList<Row> result = new LinkedList<>();
        for(Map.Entry<Integer, List<Row>> entry: globalResults.entrySet()){
            result.addAll(entry.getValue());
        }
        return result;
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        localResults.add(value);
    }

    public Row appendElementToNewRow(Row oldRow,Object... toAppendElements){
        int arity = oldRow.getArity();
        Row newRow = new Row(arity + toAppendElements.length);
        for (int i = 0; i < oldRow.getArity(); i++) {
            newRow.setField(i,oldRow.getField(i));
        }
        for (int i = 0; i <toAppendElements.length; i++) {
            newRow.setField(i,toAppendElements[arity +i]);
        }
        return newRow;
    }


    static class StreamSinkTest{

        private static volatile StreamSinkTest instance;

        private StreamSinkTest() {
            this.globalResults = new ConcurrentHashMap<>();
            this.idCounter = new AtomicInteger(0);
        }

        public static StreamSinkTest Instance(){
            if(null ==instance){
                synchronized (StreamSinkTest.class){
                    if(null ==instance){
                        instance = new StreamSinkTest();
                    }
                }
            }
            return instance;
        }

        private Map<Integer, Map<Integer, List<Row>>> globalResults;
        private AtomicInteger idCounter;

        public int newSinkId(){
            int id = idCounter.incrementAndGet();
            synchronized (this){
                this.globalResults.put(id,new ConcurrentHashMap<>());
            }
            return id;
        }

        public void putSinkIndexAndGlobalResult(int index, int taskId, List<Row> localResults) {
            Map<Integer, List<Row>> sinkGlobalResult = this.globalResults.get(index);
            if(null == sinkGlobalResult){
                synchronized (this){
                    if(!globalResults.containsKey(index)){
                        sinkGlobalResult = new ConcurrentHashMap<>();
                        globalResults.put(index,sinkGlobalResult);
                    }
                }
            }
            synchronized (this){
                sinkGlobalResult.put(taskId,localResults);
            }
        }

        public Map<Integer, List<Row>> removeSinkData(int index) {
            Map<Integer, List<Row>> removed = globalResults.remove(index);
            return removed;
        }


    }


}
