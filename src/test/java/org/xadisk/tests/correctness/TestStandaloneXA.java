/*
 Copyright © 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

 This source code is being made available to the public under the terms specified in the license
 "Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */
package org.xadisk.tests.correctness;

import com.atomikos.icatch.jta.UserTransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.bridge.proxies.interfaces.XASession;
import org.xadisk.filesystem.NativeXASession;
import org.xadisk.filesystem.SessionCommonness;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import org.xadisk.filesystem.utilities.FileIOUtility;

import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.IOException;

/*For testing, we used Atomikos (open source version) as a JTA implementation. One can get it from
 http://www.atomikos.com/Main/TransactionsEssentials .
 */
public class TestStandaloneXA {
    private static final String SEPARATOR = File.separator;
    private static final String CURRENT_WORKING_DIRECTORY = System.getProperty("user.dir") + SEPARATOR + "target" + SEPARATOR + "XADisk";
    private static final String TMP_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "tmp" + SEPARATOR;
    private static final String XA_DISK_SYSTEM_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "xa";

    @Before
    public void setupTest() {
        new File(TMP_DIRECTORY).mkdirs();
    }

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
        FileIOUtility.deleteDirectoryRecursively(new File(System.getProperty("user.dir") + SEPARATOR + "target" + SEPARATOR + "atomikos"));
    }

    @Test
    public void testStandaloneXA() {
        UserTransactionManager tm = null;
        try {
            boolean testRemote = false;
            int remotePort = 4678;
            StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "local");
            configuration.setEnableRemoteInvocations(Boolean.TRUE);
            configuration.setServerAddress("localhost");
            configuration.setServerPort(remotePort);
            XAFileSystem xafs;
            XAFileSystem nativeXAFS = XAFileSystemProxy.bootNativeXAFileSystem(configuration);
            nativeXAFS.waitForBootup(-1);
            if (testRemote) {
                xafs = XAFileSystemProxy.getRemoteXAFileSystemReference("localhost", remotePort);
            } else {
                xafs = nativeXAFS;
            }
            XASession xaSession = xafs.createSessionForXATransaction();
            XAResource xar = xaSession.getXAResource();
            tm = new UserTransactionManager();
            //UNCOMMENT ABOVE ONCE YOU BRING ATOMIKOS INTO THE CLASSPATH.
            //TransactionManager tm = null;
            tm.setTransactionTimeout(60);

            File f1 = new File(TMP_DIRECTORY + "1.txt");
            File f2 = new File(TMP_DIRECTORY + "2.txt");
            File f3 = new File(TMP_DIRECTORY + "3.txt");
            f1.delete();
            f2.delete();
            f3.delete();

            tm.begin();
            Transaction tx1 = tm.getTransaction();
            tx1.enlistResource(xar);
            xaSession.createFile(f1, false);
            tm.suspend();

            tm.begin();
            Transaction tx2 = tm.getTransaction();
            tx2.enlistResource(xar);
            xaSession.createFile(f2, false);
            tm.commit();

            tm.resume(tx1);
            tm.commit();

            tm.begin();
            Transaction tx3 = tm.getTransaction();
            tx3.enlistResource(xar);
            xaSession.createFile(f3, false);
            ((SessionCommonness) ((NativeXASession) xaSession).getSessionForCurrentWorkAssociation()).prepare();

            nativeXAFS.shutdown();
            nativeXAFS = XAFileSystemProxy.bootNativeXAFileSystem(configuration);

            if (testRemote) {
                xafs = XAFileSystemProxy.getRemoteXAFileSystemReference("localhost", remotePort);
            } else {
                xafs = nativeXAFS;
            }
            xar = xafs.getXAResourceForRecovery();
            Xid xids[] = xar.recover(XAResource.TMSTARTRSCAN);
            System.out.println(xids.length);
            xar.commit(xids[0], true);

            nativeXAFS.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tm != null) {
                tm.close();
            }
        }
    }
}
