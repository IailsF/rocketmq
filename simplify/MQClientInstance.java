package simplify;

import java.util.concurrent.*;

class MQClientInstance {

    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();

    // 用于请求broker，以及处理broker的请求
    private final MQClientAPIImpl mQClientAPIImpl;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });

    // 管理pullrequest queue，从queue中取出pullrequest，调用consumer的pullMessage方法来消费消息
    private final PullMessageService pullMessageService;
    // 定时执行rebalance
    private final RebalanceService rebalanceService;

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);
//
        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }
//
//        this.mQAdminImpl = new MQAdminImpl(this);

        // 这是一个单独的线程，用于管理并取出pullrequest队列，调用consumer的pullMessage来消费消息
        this.pullMessageService = new PullMessageService(this);

        // 这是一个单独的线程
        this.rebalanceService = new RebalanceService(this);

//        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
//        this.defaultMQProducer.resetClientConfig(clientConfig);

//        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);
    }

    public void start() throws MQClientException {
        // If not specified,looking address from name server
//        if (null == this.clientConfig.getNamesrvAddr()) {
//            this.mQClientAPIImpl.fetchNameServerAddr();
//        }
//        // Start request-response channel
//        this.mQClientAPIImpl.start();
        // Start various schedule tasks
        this.startScheduledTask();
        // Start pull service
        this.pullMessageService.start();
        // 启动一个每20秒执行一次的rebalance线程
        this.rebalanceService.start();
        // Start push service
//        this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
    }

    private void startScheduledTask() {
//        if (null == this.clientConfig.getNamesrvAddr()) {
//            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
//
//                @Override
//                public void run() {
//                    try {
//                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
//                    } catch (Exception e) {
//                        log.error("ScheduledTask fetchNameServerAddr exception", e);
//                    }
//                }
//            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
//        }

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    // 获取consumer订阅的topic，挨个调用updateTopicRouteInfoFromNameServer获取topic的路由信息
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    // 这里会将这个client下的所有producer和consumer的信息发送给broker
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    // 这里会将这个client下所有的consumer的offset信息持久化到broker
                    // 依次调用各个consumer的ConsumerImpl#persistConsumerOffset
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
    }
}
