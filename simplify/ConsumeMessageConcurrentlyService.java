package simplify;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    // consumeExecutor 线程池的任务队列，ConsumeRequest 会被放入该队列
    private final BlockingQueue<Runnable> consumeRequestQueue;
    // 用于执行 messageListener 来消费消息的线程池
    private final ThreadPoolExecutor consumeExecutor;
    // 用于延迟一段时间将ConsumeRequest放入consumeExecutor的延迟手段
    // 当提交ConsumeRequest到consumeExecutor时，如果任务满了，会用到这个来延迟提交
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate((Runnable) () -> {
            try {
                cleanExpireMsg();
            } catch (Throwable e) {
                log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
            }
        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    // 将消息按批次分割，每批创建一个 ConsumeRequest 放入 consumeExecutor 线程池去执行
    // 批次大小 DefaultMQPushConsumer.consumeMessageBatchMaxSize
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {

    }

    // 消息消费任务，放到线程池中执行
    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        // 会调用 messageListener 的 consumeMessage 方法消费消息
        // 如果消费结果是consumer_later，那么会调用 defaultMQPushConsumerImpl#sendMessageBack
        // sendBack失败的消息，会延迟5秒放入consumeExecutor的任务队列中，在本地重试
        // defaultMQPushConsumerImpl#sendMessageBack
        // -> defaultMQPushConsumerImpl.mQClientFactory.mQClientAPIImpl#consumerSendMessageBack
        //  这个方法是远程调用broker, 请求码是 RequestCode.CONSUMER_SEND_MSG_BACK
        // -> defaultMQPushConsumerImpl.mQClientFactory.mQClientAPIImpl.remotingClient#invokeSync
        // -> 最后走到broker中的 SendMessageProcessor#asyncConsumerSendMsgBack:
        //     如果消息的重试次数大于最大重试次数，那么将消息放入死信队列%DLQ%<consumer_group>,
        //     否则发给延迟队列，根据重试次数选择对应的延迟等级，延迟一段时间后放入%RETRY%<consumer_group>的队列，本consumer会消费这个队列
        run() {
        }
    }

}
