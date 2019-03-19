/*
 Copyright © 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

 This source code is being made available to the public under the terms specified in the license
 "Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */
package org.xadisk.tests.correctness;

import org.junit.After;
import org.junit.Test;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.bridge.proxies.interfaces.XASession;
import org.xadisk.connector.inbound.EndPointActivation;
import org.xadisk.connector.inbound.XADiskActivationSpecImpl;
import org.xadisk.filesystem.NativeXAFileSystem;
import org.xadisk.filesystem.TransactionInformation;
import org.xadisk.filesystem.XAFileSystemCommonness;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import org.xadisk.filesystem.utilities.FileIOUtility;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.IOException;

public class TestXAResourceImpls {
    private static final String SEPARATOR = File.separator;
    private static final String CURRENT_WORKING_DIRECTORY = System.getProperty("user.dir") + SEPARATOR + "target" + SEPARATOR + "XADisk";
    private static final String XA_DISK_SYSTEM_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "XADiskSystem";
    private static long txnId = System.currentTimeMillis();

    @After
    public void shutdownXaDisk() throws IOException {
        final XAFileSystem localXaFileSystem = XAFileSystemProxy.getNativeXAFileSystemReference("local");
        if (localXaFileSystem != null) {
            try {
                localXaFileSystem.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // Delete test files/directories
        FileIOUtility.deleteDirectoryRecursively(new File(CURRENT_WORKING_DIRECTORY));
    }

    @Test
    public void testXaResourceImpls() {
        main();
    }

    public static void main() {
        XAFileSystemCommonness xafs = null;
        try {
            TestUtility.cleanupDirectory(new File(CURRENT_WORKING_DIRECTORY));
            boolean commitDuringRecovery = true;
            File f[] = new File[3];
            for (int i = 0; i < 3; i++) {
                File dir = new File(CURRENT_WORKING_DIRECTORY + SEPARATOR + i);
                FileIOUtility.deleteDirectoryRecursively(dir);
                FileIOUtility.createDirectoriesIfRequired(dir);
                f[i] = new File(dir, "a.txt");
                if (f[i].exists()) {
                    FileIOUtility.deleteFile(f[i]);
                }
            }

            xafs = bootXAFileSystemCompletely();

            XASession xaSession = xafs.createSessionForXATransaction();
            XAResource xar = xaSession.getXAResource();
            Xid xid = getNewXid();
            xar.start(xid, XAResource.TMNOFLAGS);
            xaSession.createFile(f[0], false);
            xar.end(xid, XAResource.TMSUCCESS);
            assertNoCommit(f[0]);

            xaSession = xafs.createSessionForXATransaction();
            xar = xaSession.getXAResource();
            xid = getNewXid();
            xar.start(xid, XAResource.TMNOFLAGS);
            xaSession.createFile(f[1], false);
            xar.end(xid, XAResource.TMSUCCESS);
            xar.prepare(xid);
            assertNoCommit(f[1]);

            xaSession = xafs.createSessionForXATransaction();
            xar = xaSession.getXAResource();
            xid = getNewXid();
            xar.start(xid, XAResource.TMNOFLAGS);
            xaSession.createFile(f[2], false);
            xar.end(xid, XAResource.TMSUCCESS);
            xar.prepare(xid);
            xar.commit(xid, false);
            assertCommit(f[2]);

            xafs.shutdown();
            xafs = bootXAFileSystem();
            Thread.sleep(1000);

            xar = xafs.getXAResourceForRecovery();
            Xid[] preparedXids = xar.recover(XAResource.TMSTARTRSCAN);
            for (Xid prepared : preparedXids) {
                if (commitDuringRecovery) {
                    xar.commit(prepared, true);
                } else {
                    xar.rollback(prepared);
                }
            }

            assertNoCommit(f[0]);
            if (commitDuringRecovery) {
                assertCommit(f[1]);
                assertCommit(f[2]);
            }

            xafs.shutdown();
            xafs = bootXAFileSystemCompletely();

            for (int i = 0; i < 3; i++) {
                if (f[i].exists()) {
                    FileIOUtility.deleteFile(f[i]);
                }
            }

            SimulatedMessageEndpointFactory smef = new SimulatedMessageEndpointFactory();
            smef.goTill = SimulatedMessageEndpointFactory.GoTill.consume;
            XADiskActivationSpecImpl as = new XADiskActivationSpecImpl();
            as.setAreFilesRemote("false");
            as.setFileNamesAndEventInterests(new File(CURRENT_WORKING_DIRECTORY + SEPARATOR + 0).getAbsolutePath()
                    + "::111");
            xafs.registerEndPointActivation(new EndPointActivation(smef, as));

            smef = new SimulatedMessageEndpointFactory();
            smef.goTill = SimulatedMessageEndpointFactory.GoTill.prepare;
            as = new XADiskActivationSpecImpl();
            as.setAreFilesRemote("false");
            as.setFileNamesAndEventInterests(new File(CURRENT_WORKING_DIRECTORY + SEPARATOR + 1).getAbsolutePath()
                    + "::111");
            xafs.registerEndPointActivation(new EndPointActivation(smef, as));

            smef = new SimulatedMessageEndpointFactory();
            smef.goTill = SimulatedMessageEndpointFactory.GoTill.commit;
            as = new XADiskActivationSpecImpl();
            as.setAreFilesRemote("false");
            as.setFileNamesAndEventInterests(new File(CURRENT_WORKING_DIRECTORY + SEPARATOR + 2).getAbsolutePath()
                    + "::111");
            xafs.registerEndPointActivation(new EndPointActivation(smef, as));

            xaSession = xafs.createSessionForXATransaction();
            xar = xaSession.getXAResource();
            xid = getNewXid();
            xar.start(xid, XAResource.TMNOFLAGS);
            xaSession.createFile(f[0], false);
            xaSession.createFile(f[1], false);
            xaSession.createFile(f[2], false);
            xaSession.setPublishFileStateChangeEventsOnCommit(true);
            xar.end(xid, XAResource.TMSUCCESS);
            xar.prepare(xid);
            xar.commit(xid, false);

            Thread.sleep(1000);//to let the events get raised to MEPs.

            xafs.shutdown();
            xafs = bootXAFileSystem();
            Thread.sleep(1000);

            xar = xafs.getEventProcessingXAResourceForRecovery();
            preparedXids = xar.recover(XAResource.TMSTARTRSCAN);
            if (preparedXids.length != 1) {
                throw new AssertionError();
            }

            System.out.println("Completing the only one expected prepared txn for inbound case.");
            if (commitDuringRecovery) {
                xar.commit(preparedXids[0], true);
                System.out.println("Committed");
            } else {
                xar.rollback(preparedXids[0]);
                System.out.println("Rolled-back");
            }

            xafs.shutdown();
            xafs = bootXAFileSystemCompletely();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                xafs.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static Xid getNewXid() {
        return TransactionInformation.getXidInstanceForLocalTransaction(txnId++);
    }

    private static void assertCommit(File f) {
        if (!f.exists()) {
            throw new AssertionError();
        }
    }

    private static void assertNoCommit(File f) {
        if (f.exists()) {
            throw new AssertionError();
        }
    }

    private static XAFileSystemCommonness bootXAFileSystem() throws Exception {
        XAFileSystemCommonness xaFS;
        StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "local");
        configuration.setServerPort(9998);
        xaFS = NativeXAFileSystem.bootXAFileSystemStandAlone(configuration);
        return xaFS;
    }

    private static XAFileSystemCommonness bootXAFileSystemCompletely() throws Exception {
        XAFileSystemCommonness xaFS = bootXAFileSystem();
        xaFS.waitForBootup(-1L);
        return xaFS;
    }
}
