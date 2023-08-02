package simplify;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            // 等待waitInterval毫秒，默认20秒
            this.waitForRunning(waitInterval);
            // 这个方法会取出所有的消费者，然后调用DefaultMQPushConsumerImpl#doRebalance
            // -> RebalanceImpl#doRebalance
            // -> 根据topic 挨个调用 RebalanceImpl#rebalanceByTopic
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

}
