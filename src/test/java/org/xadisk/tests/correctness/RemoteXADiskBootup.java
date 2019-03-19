/*
 Copyright © 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

 This source code is being made available to the public under the terms specified in the license
 "Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */
package org.xadisk.tests.correctness;

import java.io.File;
import org.xadisk.filesystem.NativeXAFileSystem;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;

public class RemoteXADiskBootup {
    public static final int DEFAULT_PORT = 5151;
    public static boolean cleanSystemDir = false;
    private static final String SEPARATOR = File.separator;
    private static final String CURRENT_WORKING_DIRECTORY = System.getProperty("user.dir") + SEPARATOR + "target" + SEPARATOR + "XADisk";
    static final String XA_DISK_SYSTEM_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "XADiskSystemRemote#" + DEFAULT_PORT;

    public static void main(String args[]) {
        try {
            if (cleanSystemDir) {
                TestUtility.cleanupDirectory(new File(XA_DISK_SYSTEM_DIRECTORY));
            }
            StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "remote");
            configuration.setWorkManagerCorePoolSize(100);
            configuration.setWorkManagerMaxPoolSize(100);
            configuration.setTransactionTimeout(Integer.MAX_VALUE);
            configuration.setServerPort(DEFAULT_PORT);
            configuration.setEnableRemoteInvocations(true);
            configuration.setDeadLockDetectorInterval(1);
            NativeXAFileSystem xaFileSystem = NativeXAFileSystem.bootXAFileSystemStandAlone(configuration);
            xaFileSystem.waitForBootup(2000);

            System.out.println("XADisk System is up for use...");
        } catch (Throwable t) {
            System.err.println(t);
        }
    }
}
