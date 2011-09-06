package com.yahoo.bookkeeper.test;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class TestThroughputLatency implements AddCallback {
    static Logger LOG = Logger.getLogger(TestThroughputLatency.class);

    BookKeeper bk;
    LedgerHandle lh;
    AtomicLong counter;
    Semaphore sem;
    long times;
    long length;
    
    class Context {
        long localStartTime;
        long globalStartTime;
        long id;
        
        Context(long id, long time){
            this.id = id;
            this.localStartTime = this.globalStartTime = time;
        }
    }
    
    public TestThroughputLatency(String times, String length, String ensemble, String qSize, String throttle, String servers) 
    throws KeeperException, 
        IOException, 
        InterruptedException {
        //this.sem = new Semaphore(Integer.parseInt(throttle));
        System.setProperty("throttle", throttle);
        bk = new BookKeeper(servers);
        this.times = Long.parseLong(times);
        this.length = Long.parseLong(length);
        this.counter = new AtomicLong(0);
        try{
            //System.setProperty("throttle", throttle.toString());
            lh = bk.createLedger(Integer.parseInt(ensemble), Integer.parseInt(qSize), BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
        } catch (BKException e) {
            e.printStackTrace();
        } 
    }
    
    public void close() 
    throws InterruptedException {
        lh.close();
        bk.halt();
    }
    
    long previous = 0;
    
    void run(String data) throws InterruptedException {
        byte[] bytes = data.getBytes();
        LOG.info("Running...");
        
        long start = previous = System.currentTimeMillis();
        while(times-- > 0){
            //sem.acquire();
            lh.asyncAddEntry(bytes, this, new Context(counter.getAndIncrement(), System.nanoTime()));
        }
        
        synchronized (this) {
            if(this.counter.get() != 0)
                wait();
        }
        LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
        LOG.debug("Ended computation");
        System.out.flush();
    }
    
    long threshold = 20000;
    long runningAverage = 0;
    long runningAverageCounter = 1;
    long totalTime = 0;
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        Context context = (Context) ctx;
        
        if((entryId % 500) == 0){ 
            long newTime = System.nanoTime() - context.localStartTime;
            totalTime += newTime; 
            ++runningAverageCounter;
            //runningAverage = (newTime + ((runningAverageCounter) * runningAverage))/(++runningAverageCounter);
        }
            
        if(threshold - (entryId % threshold) == 1){
            long diff = System.currentTimeMillis() - previous;
            long toOutput = entryId + 1;
            //System.out.println("SAMPLE\t" + toOutput + "\t" + diff);
            previous = System.currentTimeMillis();
            long latency = -1;
            if(runningAverageCounter > 0){
                latency = (totalTime /runningAverageCounter)/1000000;
            }
            //runningAverage = 0;
            totalTime = 0;
            runningAverageCounter = 0;
            System.out.println("SAMPLE\t" + toOutput + "\t" + diff + "\t" + latency);
        }
        
        //sem.release();
        synchronized(this) {
            if(counter.decrementAndGet() == 0)
                notify();
        }
    }
    
    /**
     * Argument 0 is the number of entries to add
     * Argument 1 is the length of entries
     * Argument 2 is the ensemble size
     * Argument 3 is the quorum size
     * Argument 4 is the throttle threshold
     * Argument 5 is the address of the ZooKeeper server
     * 
     * @param args
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    
    public static void main(String[] args) 
    throws KeeperException, IOException, InterruptedException{
        StringBuffer servers_sb = new StringBuffer();
        for (int i = 5; i < args.length; i++){
            servers_sb.append(args[i] + " ");
        }
    
        String servers = servers_sb.toString().trim().replace(' ', ',');
        LOG.warn("(Parameters received) Num of entries to add: " + args[0] + 
                ", Length: " + args[1] + ", ensemble size: " + args[2] + 
                ", quorum size" + args[3] + 
                ", throttle: " + args[4] + 
                ", zk servers: " + servers);
        
        TestThroughputLatency ttl = new TestThroughputLatency(args[0], args[1], args[2], args[3], args[4], servers);
        
        int length = Integer.parseInt(args[1]);
        StringBuffer sb = new StringBuffer();
        while(length-- > 0){
            sb.append('a');
        }
        ttl.run(sb.toString());
        ttl.close();
    }
    
    
}
