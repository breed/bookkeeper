package org.apache.bookkeeper.test;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import org.junit.*;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger recovery.
 *
 */

public class LedgerRecoveryTest extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(LedgerRecoveryTest.class);

    DigestType digestType;

    public LedgerRecoveryTest(DigestType digestType) {
        super(3);
        this.digestType = digestType;
    }

    private void testInternal(int numEntries) throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle beforelh = null;
        beforelh = bkc.createLedger(digestType, "".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        long length = (long) (numEntries * tmp.length());

        /*
         * Try to open ledger.
         */
        LedgerHandle afterlh = bkc.openLedger(beforelh.getId(), digestType, "".getBytes());

        /*
         * Check if has recovered properly.
         */
        assertTrue("Has not recovered correctly: " + afterlh.getLastAddConfirmed(),
                   afterlh.getLastAddConfirmed() == numEntries - 1);
        assertTrue("Has not set the length correctly: " + afterlh.getLength() + ", " + length,
                   afterlh.getLength() == length);
    }

    @Test
    public void testLedgerRecovery() throws Exception {
        testInternal(100);

    }

    @Test
    public void testEmptyLedgerRecoveryOne() throws Exception {
        testInternal(1);
    }

    @Test
    public void testEmptyLedgerRecovery() throws Exception {
        testInternal(0);
    }

    @Test
    public void testLedgerRecoveryWithWrongPassword() throws Exception {
        // Create a ledger
        byte[] ledgerPassword = "aaaa".getBytes();
        LedgerHandle lh = bkc.createLedger(digestType, ledgerPassword);
        // bkc.initMessageDigest("SHA1");
        long ledgerId = lh.getId();
        LOG.info("Ledger ID: " + lh.getId());
        String tmp = "BookKeeper is cool!";
        int numEntries = 30;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(tmp.getBytes());
        }

        // Using wrong password
        ledgerPassword = "bbbb".getBytes();
        try {
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            fail("Opening ledger with wrong password should fail");
        } catch (BKException e) {
            // should failed
        }
    }

    @Test
    public void testLedgerRecoveryWithNotEnoughBookies() throws Exception {
        int numEntries = 3;

        // Create a ledger
        LedgerHandle beforelh = null;
        beforelh = bkc.createLedger(3, 3, digestType, "".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        // shutdown first bookie server
        bs.get(0).shutdown();
        bs.remove(0);

        /*
         * Try to open ledger.
         */
        try {
            bkc.openLedger(beforelh.getId(), digestType, "".getBytes());
            fail("should not reach here!");
        } catch (Exception e) {
            // should thrown recovery exception
        }

        // start a new bookie server
        int newBookiePort = initialPort + numBookies;
        startNewBookie(newBookiePort);

        LedgerHandle afterlh = bkc.openLedger(beforelh.getId(), digestType, "".getBytes());

        /*
         * Check if has recovered properly.
         */
        assertEquals(numEntries - 1, afterlh.getLastAddConfirmed());
    }

}
