package userstatistics;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import models.UserStatistics;

class ProcessUserStatisticsFunction extends ProcessWindowFunction<UserStatistics, UserStatistics, String, TimeWindow> {
    private ValueStateDescriptor<UserStatistics> stateDescriptor;

    // Initializes the Descriptor
    @Override
    public void open(Configuration parameters) throws Exception {
        stateDescriptor = new ValueStateDescriptor<>("User Statistics", UserStatistics.class);
        super.open(parameters);
    }

    // Takes three paramters
    // key of the record
    // Context to get the sate. State is used to get the accumulated state
    // List of stats accumulated over a period of time
    // collector is used to emit the result
    @Override
    public void process(String emailAddress, ProcessWindowFunction<UserStatistics, UserStatistics, String, TimeWindow>.Context context, Iterable<UserStatistics> statsList, Collector<UserStatistics> collector) throws Exception {
        // The ValueState is used to maintain the state for a specific key across different processing windows.
        // key is implicitly associated with the ValueState object obtained from the context.globalState()
        ValueState<UserStatistics> state = context.globalState().getState(stateDescriptor);
        UserStatistics accumulatedStats = state.value();

        for (UserStatistics newStats: statsList) {
            if(accumulatedStats == null)
                accumulatedStats = newStats;
            else
                accumulatedStats = accumulatedStats.merge(newStats);
        }

        state.update(accumulatedStats);

        collector.collect(accumulatedStats);
    }
}
