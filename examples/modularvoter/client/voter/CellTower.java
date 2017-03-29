package voter;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.voltdb.client.*;

public class CellTower {

    private static final int MIN_CALL_TIME_SECONDS = 2;
    private static final int MAX_CALL_TIME_SECONDS = 10;
    private static final int MIN_TIME_BETWEEN_CALLS_SECONDS = 2;
    private static final int MAX_TIME_BETWEEN_CALLS_SECONDS = 30;
    private static final int KEEPALIVE_INTERVAL_SECONDS = 1;


    static enum CallState {
        // benign states
        // as long as caller is here, it will continue to make calls
        INACTIVE,
        PENDING,
        ACTIVE,
        ENDING,
        // benign failure states
        // no more calls will be placed but the statistics won't count this as a failure
        REJECTED,
        // error states
        // if a caller ever ends up in one of these, it does not try to recover
        FAILED,
        DROPPED,
    };


    class PrepaidCaller {
        // TODO: this needs to be synced with the stored procedures
        public static final byte STORED_PROC_SUCCESS = 1;


        private long uniqueID;
        private volatile CallState state; // modified by current executor, but also read by stats keeper
        private volatile boolean callScheduled; // set to true by scheduleCall(), set to false by CallEndedTask
        private int callSecondsRemaining;

        /** Asks VoltDB to initiate a call, generating a CallStartResponse.
         */
        class CallInitiateRequest implements Runnable {

            @Override
            public void run(){
                assert (state == CallState.INACTIVE);
                if (!m_stop){
                    boolean processed = false;
                    try {
                        state = CallState.PENDING; // this has to be set first; callProcedure may return after callback is called.
                        processed = voltClient.callProcedure(new CallStartResponse(), "AuthorizeCall", uniqueID);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if (!processed){
                        state = CallState.FAILED;
                    }
                }
            }
        }

        /** After VoltDB has responded to a call initiation request, schedules keep-alives or logs that the call failed.
         * @author bshaw
         */
        class CallStartResponse implements ProcedureCallback {

            @Override
            public void clientCallback(ClientResponse clientResponse) {
                assert (state == CallState.PENDING);
                if (clientResponse.getStatus() == ClientResponse.SUCCESS){
                    if (clientResponse.getAppStatus() == STORED_PROC_SUCCESS){
                        state = CallState.ACTIVE;
                        executor.schedule(new CallAliveRequest(), KEEPALIVE_INTERVAL_SECONDS, TimeUnit.SECONDS);
                    } else {
                        state = CallState.REJECTED;
                    }
                } else {
                    state = CallState.FAILED;
                }
            }
        }

        /** Asks VoltDB whether or not the call should continue, generating a CallContinuationResponse.
         */
        class CallAliveRequest implements Runnable {
            @Override
            public void run() {
                assert (state == CallState.ACTIVE);
                if (!m_stop){
                    boolean processed = false;
                    try {
                        processed = voltClient.callProcedure(new CallContinuationResponse(), "ContinueCall", uniqueID);
                    } catch (IOException e) {
                    }
                    if (!processed){
                        state = CallState.FAILED;
                    }
                } else {
                    // simulation shutdown; voluntarily end the call
                    new CallCompletedTask().run();
                }
            }
        }

        /** After VoltDB has responded to a call continuation request, keep the call alive or ends it. */
        class CallContinuationResponse implements ProcedureCallback {

            @Override
            public void clientCallback(ClientResponse clientResponse) {
                assert (state == CallState.ACTIVE);
                if (clientResponse.getStatus() == ClientResponse.SUCCESS){
                    if (clientResponse.getAppStatus() == STORED_PROC_SUCCESS){
                        callSecondsRemaining--;
                        if (callSecondsRemaining > 1){
                            executor.schedule(new CallAliveRequest(), KEEPALIVE_INTERVAL_SECONDS, TimeUnit.SECONDS);
                        } else {
                            executor.schedule(new CallCompletedTask(), callSecondsRemaining, TimeUnit.SECONDS);
                        }
                    } else {
                        state = CallState.DROPPED;
                    }
                } else {
                    // Database not reachable; drop the call.
                    state = CallState.FAILED;
                }
            }
        }

        /** Informs VoltDB that a call has ended, generating a CallEndedResponse. */
        class CallCompletedTask implements Runnable {

            @Override
            public void run(){
                assert (state == CallState.ACTIVE);
                state = CallState.ENDING;
                boolean processed = false;
                try {
                    processed = voltClient.callProcedure(new CallEndedResponse(), "EndCall", uniqueID);
                } catch (IOException e) {
                }
                if (!processed){
                    state = CallState.FAILED;
                }
            }
        }

        /** After VoltDB has responded to an end call request, mark this Caller inactive so it can make another call. */
        class CallEndedResponse implements ProcedureCallback {

            @Override
            public void clientCallback(ClientResponse clientResponse){
                assert (state == CallState.ENDING);

                if (clientResponse.getStatus() == ClientResponse.SUCCESS){
                    if (clientResponse.getAppStatus() == STORED_PROC_SUCCESS){
                        state = CallState.INACTIVE;
                        callScheduled = false; // allows a new call to be scheduled by the main() thread.
                    } else {
                        state = CallState.FAILED;
                    }
                } else {
                    state = CallState.FAILED;
                }
            }
        }

        public CallState queryState(){
            return state;
        }

        /** Asynchronously queries current state of this caller.
         * @return State of the phone call
         */
        public PrepaidCaller( long uniqueID ){
            this.uniqueID = uniqueID;
            this.state = CallState.INACTIVE;
            this.callScheduled = false;
            this.callSecondsRemaining = 0;
        }

        public void scheduleCall(){
            if (!callScheduled){
                callScheduled = true;
                callSecondsRemaining = generateCallDurationSeconds();
                executor.schedule(new CallInitiateRequest(), generateCallDelaySeconds(), TimeUnit.SECONDS);
            }
        }
    }

    private final Client voltClient;
    private final ScheduledExecutorService executor;
    private long uniqueIDCounter;
    private final List<PrepaidCaller> callerList = new Vector<PrepaidCaller>();
    private volatile boolean m_stop = false; // set to true by stop()
    private final ClientStatsContext statistics;


    CellTower(Client client, long uniqueIDStart){
        voltClient = client;
        executor = Executors.newSingleThreadScheduledExecutor();
        uniqueIDCounter = uniqueIDStart;
        statistics = client.createStatsContext();
    }

    private static int generateCallDurationSeconds(){
        return ThreadLocalRandom.current().nextInt(MIN_CALL_TIME_SECONDS, MAX_CALL_TIME_SECONDS);
    }

    private static int generateCallDelaySeconds(){
        return ThreadLocalRandom.current().nextInt(MIN_TIME_BETWEEN_CALLS_SECONDS, MAX_TIME_BETWEEN_CALLS_SECONDS);
    }


    void addCaller(int totalDurationSeconds){
        PrepaidCaller caller = new PrepaidCaller( uniqueIDCounter );
        callerList.add(caller);
        caller.scheduleCall();
    }

    void scheduleMoreCalls(){
        for (PrepaidCaller caller : callerList){
            // this will not schedule a call unless the most recent one successfully completed
            caller.scheduleCall();
        }
    }

    public void stop(){
        m_stop = true;
    }

    public void cleanup(){
        // NOTE: This will cancel all scheduled "New Call" tasks - and that is OK.
        executor.shutdownNow();
        try {
            voltClient.drain();
            voltClient.close();
        } catch (InterruptedException | NoConnectionsException e){
            // unexpected, therefore crash
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void printVoltStats() {
        final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

        ClientStats stats = statistics.fetch().getStats();
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());

        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
        System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
        System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
        System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
        System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
        System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
        System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
        System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));
        System.out.printf("99.99th percentile latency:    %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9999));
        System.out.printf("Server Internal Avg latency:   %,9.2f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Latency Histogram");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(stats.latencyHistoReport());
    }

    private void computeStats( Map<CallState, Integer> stateCount ){
        for (PrepaidCaller caller : callerList){
            CallState callerState = caller.queryState();
            stateCount.put(callerState, stateCount.get(callerState) + 1);
        }
    }

    public static void printStats(Collection<CellTower> cellTowerList){
        Map<CallState, Integer> stateCount = new EnumMap<CallState, Integer>(CallState.class);
        for (CallState state : CallState.class.getEnumConstants()){
            stateCount.put(state, 0);
        }
        for (CellTower cellTower : cellTowerList){
            cellTower.computeStats(stateCount);
        }
        for (Map.Entry<CallState, Integer> entry : stateCount.entrySet()){
            System.out.println("    " + entry.getKey().toString() + ": " + entry.getValue().toString());
        }
    }


    public static void main(String[] args){
        final int NUM_VOLT_CLIENTS = 2;
        final int NUM_CALLERS_PER_EXECUTOR = 500000;
        final int TEST_DURATION_SECONDS = 30;

        List<CellTower> cellTowerList = new Vector<CellTower>();

        try {
            for (int clientIdx = 0; clientIdx < NUM_VOLT_CLIENTS; clientIdx++){
                Client voltClient = ClientFactory.createClient();
                voltClient.createConnection("localhost");
                CellTower cellTower = new CellTower(voltClient, clientIdx * NUM_CALLERS_PER_EXECUTOR);
                cellTowerList.add(cellTower);
            }

            for (int i = 0; i < NUM_CALLERS_PER_EXECUTOR; i++){
                for (CellTower cellTower : cellTowerList){
                    cellTower.addCaller(TEST_DURATION_SECONDS);
                }
            }

            System.out.println("Running simulation");
            for (int i = 0; i < TEST_DURATION_SECONDS; i++){
                for (CellTower cellTower : cellTowerList){
                    cellTower.scheduleMoreCalls();
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // should not happen; ignore
                }
                System.out.println("Statistics at " + (i + 1) + " of " + TEST_DURATION_SECONDS + " seconds:");
                printStats(cellTowerList);
            }


            System.out.println("Waiting for calls to complete");
            for (CellTower cellTower : cellTowerList){
                cellTower.stop();
            }

            final int WAIT_TIME_SECS = KEEPALIVE_INTERVAL_SECONDS * 2;
            for (int i = 0; i < WAIT_TIME_SECS - 1; i++){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // should not happen; ignore
                }
                System.out.println("Statistics at " + (i + 1) + " of " + WAIT_TIME_SECS + " seconds:");
                printStats(cellTowerList);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // should not happen; ignore
            }
            System.out.println("Final statistics:");
            printStats(cellTowerList);

        } catch (IOException e){
            e.printStackTrace();
            System.err.println("Could not connect to VoltDB at " + "localhost");
            System.exit(-1);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            for (CellTower cellTower : cellTowerList){
                cellTower.printVoltStats();
                cellTower.cleanup();
            }
        }
    }
}