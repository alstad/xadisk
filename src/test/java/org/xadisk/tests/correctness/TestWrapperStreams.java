/*
 Copyright © 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

 This source code is being made available to the public under the terms specified in the license
 "Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */
package org.xadisk.tests.correctness;

import org.junit.After;
import org.junit.Before;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestWrapperStreams {
    private static final String SEPARATOR = File.separator;
    private static final String CURRENT_WORKING_DIRECTORY = System.getProperty("user.dir") + SEPARATOR + "target" + SEPARATOR + "XADisk";
    private static final String TMP_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "tmp" + SEPARATOR;
    private static final String XA_DISK_SYSTEM_DIRECTORY = CURRENT_WORKING_DIRECTORY + SEPARATOR + "XADiskSystem";

    @Before
    public void setupTest() throws IOException {
        new File(TMP_DIRECTORY).mkdirs();
        Files.write(Paths.get(TMP_DIRECTORY, "a.txt"), "abcdef".getBytes(StandardCharsets.UTF_8), CREATE);
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
    }

    @Test
    public void testReadingAndWritingThroughStreams() throws Exception {
        StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "local");
        configuration.setWorkManagerCorePoolSize(100);
        configuration.setWorkManagerMaxPoolSize(100);
        configuration.setServerPort(9998);

        NativeXAFileSystem xaFileSystem = NativeXAFileSystem.bootXAFileSystemStandAlone(configuration);
        xaFileSystem.waitForBootup(-1L);

        Session session = xaFileSystem.createSessionForLocalTransaction();
        InputStream inputStream = new XAFileInputStreamWrapper(session.createXAFileInputStream(new File(TMP_DIRECTORY +"a.txt"), false));
        inputStream.mark(100);
        assertThat((char) inputStream.read(), is('a'));
        assertThat((char) inputStream.read(), is('b'));
        assertThat((char) inputStream.read(), is('c'));
        inputStream.reset();
        assertThat((char) inputStream.read(), is('a'));
        inputStream.close();

        final File outputFile = new File(TMP_DIRECTORY + "b.txt");
        session.createFile(outputFile, false);
        OutputStream os = new XAFileOutputStreamWrapper(session.createXAFileOutputStream(outputFile, false));
        os.write('a');
        os.write('b');
        os.close();

        assertThat("A file created inside a session/transaction should NOT be visible before commit", outputFile.exists(), is(false));
        session.commit();
        assertThat("A file created inside a session/transaction should be visible after commit", outputFile.exists(), is(true));

        xaFileSystem.shutdown();
    }
}
