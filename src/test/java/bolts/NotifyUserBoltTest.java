package bolts;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * NotifyUserBolt Tester.
 *
 * @author nolan.zhang
 * @version 1.0
 * @since <pre>Nov 22, 2017</pre>
 */

public class NotifyUserBoltTest {
    private NotifyUserBolt bolt;

    @Mock
    private TopologyContext topologyContext;

    @Mock
    private UserNotifier notifier;

    public NotifyUserBoltTest() {
    }

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);

        // The factory will return our mock `notifier`
        bolt = new NotifyUserBolt(new TestFactory(notifier));
        // Now the bolt is holding on to our mock and is under our control!
        bolt.prepare(new Config(), topologyContext);
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: prepare(Map stormConf, TopologyContext context)
     */
    @Test
    public void testPrepare() throws Exception {
        //TODO: Test goes here...
    }

    /**
     * Method: execute(Tuple input, BasicOutputCollector collector)
     */
    @Test
    public void testExecute() throws Exception {
        long userId = 24;
        Tuple tuple = mock(Tuple.class);
        BasicOutputCollector collector = mock(BasicOutputCollector.class);

        when(tuple.getLongByField(PreviousBolt.USER_ID_FIELD_NAME)).thenReturn(userId);

        bolt.execute(tuple, collector);

        // Here we just verify a call on `notifier`, but we could have stubbed out behavior befor
        //  the call to execute, too.
        verify(notifier).notifyUser(userId);
        verify(collector).emit(new Values(userId));
    }



    /**
     * Method: declareOutputFields(OutputFieldsDeclarer declarer)
     */
    @Test
    public void testDeclareOutputFields() throws Exception {
        //TODO: Test goes here...
    }

    // This test implementation allows us to get the mock to the unit-under-test.
    private class TestFactory extends UserNotifierFactory {

        private final UserNotifier notifier;

        private TestFactory(UserNotifier notifier) {
            this.notifier = notifier;
        }

        @Override
        public UserNotifier createUserNotifier() {
            return notifier;
        }
    }


} 
