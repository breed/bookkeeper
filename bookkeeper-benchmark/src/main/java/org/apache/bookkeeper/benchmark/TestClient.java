package com.yahoo.bookkeeper.test;

//import java.util.concurrent.Semaphore;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.zookeeper.KeeperException;

import com.yahoo.SyncPrimitive.SyncPrimitive.BarrierExp2;

public class TestClient 
    implements AddCallback, ReadCallback{
    static Logger LOG = Logger.getLogger(TestClient.class);
    Integer expType;
    int bytes;
    
    long previous;
    long threshold = 20000;
    BookKeeper x;
    LedgerHandle lh;
    Integer entryId;
    
    HashMap<Integer, Integer> map;
    
    FileOutputStream fStream;
    FileOutputStream fStreamLocal;
    long start, lastId;
    
    //Semaphore sem;
    
    static class Counter{
        
        long last;
        long counter;
        Counter(long last){
            this.last = last;
            this.counter = 0;
        }
        
        synchronized void inc(long step){
            this.counter += step;
        }
        
        synchronized boolean isReady(){
            return (last <= counter);
        }
    }
    
    //private final int THROTTLE = 10000;
    public TestClient() {
        entryId = 0;
        map = new HashMap<Integer, Integer>();
        this.previous = System.currentTimeMillis();
        //this.sem = new Semaphore(THROTTLE);
    }
    
    public TestClient(String servers) throws KeeperException, IOException, InterruptedException{
        this();
        x = new BookKeeper(servers);
        try{
        lh = x.createLedger(BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
        //this.sem = new Semaphore(THROTTLE);
        } catch (BKException e) {
            e.printStackTrace();
        } 
    }
    
    public TestClient(String servers, int ensSize, int qSize)
    throws KeeperException, IOException, InterruptedException, BKException{
        this();
        x = new BookKeeper(servers);
        try{
        lh = x.createLedger(ensSize, qSize, BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
        //this.sem = new Semaphore(THROTTLE);
        LOG.debug("Ledger handle: " + lh.getId());
        } catch (BKException e) {
            e.printStackTrace();
        } 
    }
    
    public TestClient(String servers, long ledgerId)
    throws KeeperException, IOException, InterruptedException, BKException{
        this();
        x = new BookKeeper(servers);
        lh = x.openLedger(ledgerId, BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
        //this.sem = new Semaphore(THROTTLE);
        LOG.debug("Ledger handle: " + lh);
    }
    
    public TestClient(FileOutputStream fStream)
    throws FileNotFoundException {
        this.fStream = fStream;
        this.fStreamLocal = new FileOutputStream("./local.log");
        //this.sem = new Semaphore(THROTTLE);
    }
    
    LedgerHandle[] lhArray;
    public TestClient(String servers, int ensSize, int qSize, int numLedgers)
    throws KeeperException, IOException, InterruptedException, BKException{
        this();
        Long throttle = (((Double) Math.max(1.0, ((double) 10000/numLedgers))).longValue());
        System.setProperty("throttle", throttle.toString());
        x = new BookKeeper(servers);
        lhArray = new LedgerHandle[numLedgers];
        try{
            for(int i = 0; i < numLedgers; i++){
                lhArray[i] = x.createLedger(ensSize, qSize, BookKeeper.DigestType.CRC32, new byte[] {'a', 'b'});
                LOG.debug("Ledger handle: " + lhArray[i].getId());
            }
            LOG.info("Done creating ledgers.");
        } catch (BKException e) {
            e.printStackTrace();
        } 
    }
    
    public Integer getFreshEntryId(int val){
        ++this.entryId;
        synchronized (map) {
            map.put(this.entryId, val);
        }
        return this.entryId;
    }
    
    public boolean removeEntryId(Integer id){
        boolean retVal = false;
        synchronized (map) {
            map.remove(id);
            retVal = true;
     
            if(map.size() == 0) map.notifyAll();
            else{
                if(map.size() < 4)
                    LOG.debug(map.toString());
            }
        }
        return retVal;
    }

    public void close() throws KeeperException, InterruptedException{
        LOG.info("Closing experiment.");
        if(lhArray != null){
            for(int i = 0; i < lhArray.length; i++){
                lhArray[i].close();
            }
        } else {
            lh.close();
        }
        LOG.debug("Done closing ledgers.");
        x.halt();
        LOG.debug("Done halting bookkeper object.");
    }
    
    /**
     * The parameters depend on the experiment selected.
     * 
     * 0: Write generic
     * 1: Write to local file system
     * 2: Write verifiable
     * 3: Coordinated writes, multiple clients writing (verifiable)
     * 4: Coordinated writes, multiple clients writing (generic)
     * 5: Read entries
     * 6: Byte throughput verifiable
     * 7: Byte throughput generic
     * 8: Overlapping reads and writes
     * 9: Overlapping reads and writes
     * 10: Coordinated byte throughput (verifiable)
     * 11: Coordinated byte throughput (generic)
     * 12: Multi ledger
     * 13: Write first then read
     * 
     * @param args
     */
    public static void main(String[] args) {
        
        int lenght = Integer.parseInt(args[1]);
        StringBuffer sb = new StringBuffer();
        while(lenght-- > 0){
            sb.append('a');
        }
        
        Integer selection = Integer.parseInt(args[0]);
        StringBuffer servers_sb;
        String servers;
        
        System.out.println("Selection: " + selection);
        
        switch(selection){
        case 0:           
            servers_sb = new StringBuffer();
            for (int i = 5; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                /*int lenght = Integer.parseInt(args[1]);
                StringBuffer sb = new StringBuffer();
                while(lenght-- > 0){
                    sb.append('a');
                }*/
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                c.writeSameEntryBatch(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                c.close();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
            	e.printStackTrace();
            }
            break;
        case 1:
            
            try{
                TestClient c = new TestClient(new FileOutputStream(args[2]));
                c.writeSameEntryBatchFS(selection, sb.toString().getBytes(), Integer.parseInt(args[3]));
                c.close();
            } catch(FileNotFoundException e){
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            break;
        case 2:
            servers_sb = new StringBuffer();
            for (int i = 5; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                System.out.println("######## Creating new client ##########");
                System.setProperty("throttle", "18000");
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                c.writeSameEntryBatchVerifiable(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                ////c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                LOG.debug("Closing handle");
                c.close();
                LOG.debug("Closed handle");
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
        case 3:
            servers_sb = new StringBuffer();
            for (int i = 6; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                System.out.println("ZooKeeper server: " + servers);
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                BarrierExp2 b = new BarrierExp2(servers, "/b", "10", new Integer(args[5]));
                b.enter();
                c.writeSameEntryBatchVerifiable(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                c.close();
                b.leave();
                b.close();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            } 
            break;
        case 4:
            servers_sb = new StringBuffer();
            for (int i = 6; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                BarrierExp2 b = new BarrierExp2(args[6], "/b", "10", new Integer(args[5]));
                b.enter();
                c.writeSameEntryBatchVerifiable(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                c.close();
                b.leave();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
        case 5:
            servers_sb = new StringBuffer();
            for (int i = 3; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
            servers = servers_sb.toString().trim().replace(' ', ',');
            try{   
                TestClient c = new TestClient(servers, Long.parseLong(args[2]));
                c.readEntries(selection);
                c.close();
            } catch (BKException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            break;
        case 6:
            servers_sb = new StringBuffer();
            for (int i = 5; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                c.writeSameEntryBatchByte(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                LOG.debug("Closing handle");
                c.close();
                LOG.debug("Closed handle");
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
        case 7:
            servers_sb = new StringBuffer();
            for (int i = 5; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                c.writeSameEntryBatchByteGeneric(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                LOG.debug("Closing handle");
                c.close();
                LOG.debug("Closed handle");
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
            /*
             * This case corresponds to the read-write experiment in which a client
             * reads from a ledger while another client writes to a ledger. Case 9
             * starts and executes the writer.
             */
            
        case 8:
            servers_sb = new StringBuffer();
            for (int i = 5; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                BarrierExp2 b = new BarrierExp2(servers, "/b-rw", "10", 2);
                b.enter();
                c.writeSameEntryBatchVerifiable(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                c.close();
                b.leave();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
            
            /*
             * This case corresponds to the read-write experiment in which a client
             * reads from a ledger while another client writes to a ledger. Case 9
             * starts and executes the reader.
             */
        case 9:
            servers_sb = new StringBuffer();
            for (int i = 3; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
        
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
            	TestClient c = new TestClient(servers, Integer.parseInt(args[2]));
            	
                BarrierExp2 b = new BarrierExp2(servers, "/b-rw", "10", 2);
                b.enter();
                c.readEntries(selection);
                c.close();
                b.leave();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
        case 10:
        	servers_sb = new StringBuffer();
        	for (int i = 6; i < args.length; i++){
        		servers_sb.append(args[i] + " ");
        	}
        	servers = servers_sb.toString().trim().replace(' ', ',');
        	try {
        		TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        		BarrierExp2 b = new BarrierExp2(args[6], "/b-cv", "10", new Integer(args[5]));
                b.enter();
                c.writeSameEntryBatchByte(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                c.close();
                b.leave();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
            
        case 11:
        	servers_sb = new StringBuffer();
        	for (int i = 6; i < args.length; i++){
        		servers_sb.append(args[i] + " ");
        	}
        	servers = servers_sb.toString().trim().replace(' ', ',');
        	try {
        		TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        		BarrierExp2 b = new BarrierExp2(args[6], "/b-cg", "10", new Integer(args[5]));
                b.enter();
                c.writeSameEntryBatchByteGeneric(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                c.close();
                b.leave();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
            	e.printStackTrace();
            }
            break;
        case 12:
            servers_sb = new StringBuffer();
            for (int i = 6; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
            servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));
                c.writeSameEntryBatchMultiLedger(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                LOG.debug("Closing experiment");
                c.close();
                LOG.debug("Closed experiment");
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (BKException e){
                e.printStackTrace();
            }
            break;
        case 13:
            servers_sb = new StringBuffer();
            for (int i = 5; i < args.length; i++){
                servers_sb.append(args[i] + " ");
            }
            servers = servers_sb.toString().trim().replace(' ', ',');
            try{   
                TestClient wc = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                wc.writeSameEntryBatchVerifiable(selection, sb.toString().getBytes(), Integer.parseInt(args[2]));
                long ledgerId = wc.lh.getId();
                wc.close();
                LOG.info("Start read client");
                TestClient c = new TestClient(servers, ledgerId);
                c.readEntries(selection);
                c.close();
            } catch (BKException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            break;
        }
        
    }

    void writeSameEntryBatchVerifiable(Integer selection, byte[] data, int times) throws InterruptedException, KeeperException{
        //try{
        	expType = selection;
        	
            //x.initMessageDigest("SHA1");
            int count = times;
            LOG.debug("Data: " + new String(data) + ", " + data.length);
        
            start = System.currentTimeMillis();
            while(count-- > 0){
               //sem.acquire();
               lh.asyncAddEntry(data, this, this.getFreshEntryId(2));
            }
            //LOG.debug("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));       
            synchronized (map) {
                if(map.size() != 0)
                    map.wait();
            }
            LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
            /*Integer mon = Integer.valueOf(0);
            synchronized(mon){
            
                try{                  
                    x.asyncReadEntries(lh, 0, times - 1, this, mon);
                    mon.wait();
                } catch (BKException e){
                    LOG.error(e);
                }
        } */
            LOG.debug("Ended computation");
            System.out.flush();
        //} catch (NoSuchAlgorithmException e){
        //    e.printStackTrace();
        //} catch (IOException e) {
        //    e.printStackTrace();
        //} 
        //catch (BKException e){
        //    e.toString();
        //}
    }

    void writeSameEntryBatchByte(Integer selection, byte[] data, int bytes) throws InterruptedException, KeeperException{
        //try{
        	this.expType = selection;
        	this.bytes = data.length;
        	this.threshold = 50 * 1024 * 1024;
        	
            //x.initMessageDigest("SHA1");
            int count = bytes/data.length;
            LOG.debug("Data: " + data.length + ", " + count);
        
            start = System.currentTimeMillis();
            while(count-- > 0){
                //sem.acquire();
                lh.asyncAddEntry(data, this, this.getFreshEntryId(2));
            }
            //LOG.debug("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));       
            synchronized (map) {
                if(map.size() != 0)
                    map.wait();
            }
            LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
            LOG.debug("Ended computation");
            System.out.flush();
        //} catch (IOException e) {
        //    e.printStackTrace();
        //} 
        //catch (BKException e){
        //    e.toString();
        //}
    }
    
    void writeSameEntryBatchByteGeneric(Integer selection, byte[] data, int bytes) throws InterruptedException, KeeperException{
        //try{
        	this.expType = selection;
        	this.bytes = data.length;
        	this.threshold = 50 * 1024 * 1024;
        	
            int count = bytes/data.length;
            LOG.debug("Data: " + data.length + ", " + count);
        
            start = System.currentTimeMillis();
            while(count-- > 0){
                //sem.acquire();
                lh.asyncAddEntry(data, this, this.getFreshEntryId(2));
            }
            
            synchronized (map) {
                if(map.size() != 0)
                    map.wait();
            }
            LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
            LOG.debug("Ended computation");
            System.out.flush();
    }
    
    void writeSameEntryBatchMultiLedger(Integer selection, byte[] data, int times) throws InterruptedException, KeeperException{
        this.expType = selection;
       
        int numLedgers = lhArray.length;
        Random r = new Random();
        int count = times;
        if(LOG.isDebugEnabled()){
            LOG.debug("Data: " + data.length + ", " + count);
        }
        
        start = System.currentTimeMillis();
        while(count-- > 0){
            int nextLh = r.nextInt(numLedgers);
            lhArray[nextLh].asyncAddEntry(data, this, this.getFreshEntryId(2));
        }

        synchronized (map) {
            if(map.size() != 0)
                map.wait();
        }
        LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
        LOG.debug("Ended computation");
    }
    
    void writeSameEntryBatchByteMultiLedger(Integer selection, byte[] data, int bytes) throws InterruptedException, KeeperException{
            this.expType = selection;
            this.bytes = data.length;
            this.threshold = 50 * 1024 * 1024;
            
            int numLedgers = lhArray.length;
            Random r = new Random();
            int count = bytes/data.length;
            LOG.debug("Data: " + data.length + ", " + count);
        
            start = System.currentTimeMillis();
            while(count-- > 0){
                int nextLh = r.nextInt(numLedgers);
                lhArray[nextLh].asyncAddEntry(data, this, this.getFreshEntryId(2));
            }
   
            synchronized (map) {
                if(map.size() != 0)
                    map.wait();
            }
            LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
            LOG.debug("Ended computation");
    }
    
    
    void writeSameEntryBatch(Integer selection, byte[] data, int times)
    throws InterruptedException, KeeperException, IOException, NoSuchAlgorithmException {
    	expType = selection;
    	
    	int count = times;
        LOG.debug("Data: " + new String(data) + ", " + data.length);
        
        start = System.currentTimeMillis();
        while(count-- > 0){
            //sem.acquire();
            lh.asyncAddEntry(data, this, this.getFreshEntryId(2));
        }
        //LOG.debug("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));       
        synchronized (map) {
            if(map.size() != 0)
                map.wait();
        }
        LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));
        
        /*Integer mon = Integer.valueOf(0);
        synchronized(mon){
            
                try{                  
                    x.asyncReadEntries(lh, 0, times - 1, this, mon);
                    mon.wait();
                } catch (BKException e){
                    LOG.error(e);
                }
        } */
        LOG.error("Ended computation");
        System.out.flush();
    }
        
        
    void writeConsecutiveEntriesBatch(int times) 
    throws InterruptedException, IOException, NoSuchAlgorithmException {
        start = System.currentTimeMillis();
        int count = times;
        while(count-- > 0){
            byte[] write = new byte[2];
            int j = count%100;
            int k = (count+1)%100;
            write[0] = (byte) j;
            write[1] = (byte) k;
            if(count == 10000) Thread.sleep(1000);
            lh.asyncAddEntry(write, this, this.getFreshEntryId(2));
        }
        LOG.debug("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));       
        synchronized (map) {
            if(map.size() != 0)
                map.wait();
        }
        LOG.debug("Finished processing writes (ms): " + (System.currentTimeMillis() - start));
        System.out.flush();
        /*Integer mon = Integer.valueOf(0);
        synchronized(mon){
            try{
                x.asyncReadEntries(lh, 1, times - 1, this, mon);
                mon.wait();
            } catch (BKException e){
                LOG.error(e);
            }
        }*/
        LOG.error("Ended computation");
    }

    void writeSameEntryBatchFS(Integer selection, byte[] data, int times) {
    	expType = selection;
    	
    	int count = times;
        System.out.println("Data: " + data.length + ", " + times);
        long padding = 4*1024*1024;
        long padcounter = padding;
        try{
            fStream.getChannel().write(ByteBuffer.wrap(new byte[1]), padding);

            start = System.currentTimeMillis();
            while(count-- > 0){
                fStream.write(data);
                //fStreamLocal.write(data);
                if((count % 5) == 0){
                    fStream.getChannel().force(false);
                }
                if((count % 20000) == 0){
                    System.out.println("20000\t" + (System.currentTimeMillis() - start));
                    start = System.currentTimeMillis();
                }
                if(fStream.getChannel().position() > padcounter){
                    padcounter += padding;
                    fStream.getChannel().write(ByteBuffer.wrap(new byte[1]), padcounter);
                    //System.out.println(count + "\t" + padcounter);
                }
            }
            //fStream.flush();
            fStream.close();
            System.out.println("Finished processing writes (ms): " + (System.currentTimeMillis() - start));
        } catch(IOException e){
            e.printStackTrace();
        }
        System.out.flush();
    }
        
    void readEntries(Integer selection) throws InterruptedException, BKException, KeeperException {
    	expType = selection;
    	
    	long last = lh.getLastAddConfirmed();
        
        LOG.info("Last: " + last);
        Counter c = new Counter(last);
        start = System.currentTimeMillis();
        for(long i = 0; i <= (last - 19999); i+=20000){
            lh.asyncReadEntries(i, i+19999, this, c);
            LOG.debug("Requesting another batch: " + i);
        }
        LOG.debug("Done requesting batches");
        synchronized(c){
            c.wait();
        }
        LOG.debug("Finished processing writes (ms): " + (System.currentTimeMillis() - start));
        //x.closeLedger(lh);
    }
    
    long cumulative = 0;
    AtomicLong entryCounter = new AtomicLong(0);
    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        this.removeEntryId((Integer) ctx);
        
        if(LOG.isDebugEnabled()){
            LOG.debug("Completed entry " + entryId + " of ledger " + lh.getId());
        }
        
        if(rc != 0)
        	LOG.error("Error: " + rc);
        long value;
        //sem.release();
        if(expType == 13) return;
        if((expType == 6) || 
        		(expType == 7) ||
        		(expType == 10)){
        	cumulative += bytes;
        	value = cumulative;
        	
        	//long tmp = value % threshold;
        	//LOG.debug(entryId);
        	if((value % threshold) == 0){
                long diff = System.currentTimeMillis() - previous;
                long toOutput = value + bytes;
                System.out.println(toOutput + "\t" + diff);
                previous = System.currentTimeMillis();
            }
        }
        else if(expType == 12){
            value = entryCounter.getAndIncrement();
            if((threshold - (value % threshold)) == 1){
                long diff = System.currentTimeMillis() - previous;
                long toOutput = value + 1;
                System.out.println("SAMPLE\t" + toOutput + "\t" + diff);
                previous = System.currentTimeMillis();
            }
        }
        else{
            
        	value = entryId;
        	if((threshold - (value % threshold)) == 1){
                long diff = System.currentTimeMillis() - previous;
                long toOutput = value + 1;
                System.out.println("SAMPLE\t" + toOutput + "\t" + diff);
                previous = System.currentTimeMillis();
            }
        }        
    }
    
    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx){
        
    	long diff = System.currentTimeMillis() - previous;
    	int i = 0;
    	long toOutput = 0;
    	while(seq.hasMoreElements()){
    	    i++;
    	    toOutput = seq.nextElement().getEntryId();
    	}
        System.out.println("SAMPLE\t" + toOutput + "\t" + diff);
        previous = System.currentTimeMillis();
        
        Counter c = (Counter) ctx;
        c.inc(20000);
        synchronized(c){
            if(c.isReady())
                c.notify();
        }
    }
}