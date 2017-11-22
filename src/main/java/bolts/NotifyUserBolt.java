package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NotifyUserBolt extends BaseBasicBolt {
    public static final String NAME = "NotifyUser";
    private static final String USER_ID_FIELD_NAME = "userId";

    private final UserNotifierFactory factory;
    transient private UserNotifier notifier;

    public NotifyUserBolt(UserNotifierFactory factory) {
        checkNotNull(factory);

        this.factory = factory;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        notifier = factory.createUserNotifier();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // This check ensures that the time-dependency imposed by Storm has been observed
        checkState(notifier != null, "Unable to execute because user notifier is unavailable.  Was this bolt successfully prepared?");

        long userId = input.getLongByField(PreviousBolt.USER_ID_FIELD_NAME);

        notifier.notifyUser(userId);

        collector.emit(new Values(userId));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(USER_ID_FIELD_NAME));
    }
}