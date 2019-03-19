/*
 Copyright © 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

 This source code is being made available to the public under the terms specified in the license
 "Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */
package org.xadisk.tests.correctness;

import org.junit.After;
import org.junit.Test;
import org.xadisk.additional.XAFileInputStreamWrapper;
import org.xadisk.additional.XAFileOutputStreamWrapper;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.filesystem.NativeXAFileSystem;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import org.xadisk.filesystem.utilities.FileIOUtility;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TestWrapperStreams {
    private static final String SEPARATOR = File.separator;
    private static final String CURRENT_WORKING_DIRECTORY = System.getProperty("user.dir") + SEPARATOR + "target" + SEPARATOR + "XADisk";
    private static final String TMP_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "tmp" + SEPARATOR;
    private static final String XA_DISK_SYSTEM_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "XADiskSystem";

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
    public void wrapperStreams() {
        try {
            StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "local");
            configuration.setWorkManagerCorePoolSize(100);
            configuration.setWorkManagerMaxPoolSize(100);
            configuration.setServerPort(9998);

            NativeXAFileSystem xaFileSystem = NativeXAFileSystem.bootXAFileSystemStandAlone(configuration);
            xaFileSystem.waitForBootup(-1L);

            Session session = xaFileSystem.createSessionForLocalTransaction();
            InputStream is = new XAFileInputStreamWrapper(session.createXAFileInputStream(new File(TMP_DIRECTORY +"a.txt"), false));
            is.mark(100);
            System.out.println((char) is.read());
            System.out.println((char) is.read());
            System.out.println((char) is.read());
            is.reset();
            System.out.println((char) is.read());
            is.close();

            OutputStream os = new XAFileOutputStreamWrapper(session.createXAFileOutputStream(new File(TMP_DIRECTORY +"b.txt"), false));
            os.write('a');
            os.write('b');
            os.close();
            session.commit();

            xaFileSystem.shutdown();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
