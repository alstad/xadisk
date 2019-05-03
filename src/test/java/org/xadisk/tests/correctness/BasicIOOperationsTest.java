package org.xadisk.tests.correctness;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xadisk.bridge.proxies.interfaces.Session;
import org.xadisk.bridge.proxies.interfaces.XAFileSystem;
import org.xadisk.bridge.proxies.interfaces.XAFileSystemProxy;
import org.xadisk.filesystem.exceptions.*;
import org.xadisk.filesystem.standalone.StandaloneFileSystemConfiguration;
import org.xadisk.filesystem.utilities.FileIOUtility;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BasicIOOperationsTest {
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
        FileIOUtility.deleteDirectoryRecursively(Paths.get(CURRENT_WORKING_DIRECTORY));
    }

    /**
     * Checking for file existence for a non-existing file inside a non-existing parent directory
     * should return false (false = does not exist).
     */
    @Test
    public void fileExists_on_file_in_nonexisting_dir() throws InterruptedException, NoTransactionAssociatedException, InsufficientPermissionOnFileException, LockingFailedException {
        StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "local");
        XAFileSystem xafs = XAFileSystemProxy.bootNativeXAFileSystem(configuration);
        xafs.waitForBootup(-1);
        Session xaSession = xafs.createSessionForLocalTransaction();
        boolean existResult = xaSession.fileExists(new File(CURRENT_WORKING_DIRECTORY, "non-existing-dir/non-existing-file.txt"));
        assertThat(existResult, is(false));
    }

    /**
     * Creating a file in a non-existing dir should create the file and the necessary parent directories. The new
     * file and directories should only exist after XASession#commit().
     */
    @Test
    public void createFile_in_nonexisting_dir() throws InterruptedException, XAApplicationException {
        StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "local");
        XAFileSystem xafs = XAFileSystemProxy.bootNativeXAFileSystem(configuration);
        xafs.waitForBootup(-1);
        Session xaSession = xafs.createSessionForLocalTransaction();

        final File nonExistingDir = new File(CURRENT_WORKING_DIRECTORY, "non-existing-dir");
        assertThat(nonExistingDir.exists(), is(false));

        final File nonExistingFile = new File(CURRENT_WORKING_DIRECTORY, "non-existing-dir/non-existing-file.txt");
        xaSession.createFile(nonExistingFile, false);
        // The file and its parent folder should only exist inside the transaction at this point in time
        assertThat(nonExistingDir.exists(), is(false));
        assertThat(nonExistingFile.exists(), is(false));

        xaSession.commit();
        assertThat(nonExistingDir.exists(), is(true));
        assertThat(nonExistingFile.exists(), is(true));
    }

    /**
     * Checking for file existence for a non-existing file inside a parent-of-parent directory created within the current session.
     */
    @Test
    public void fileExists_on_file_in_tracked_session_dir() throws InterruptedException, NoTransactionAssociatedException, InsufficientPermissionOnFileException, LockingFailedException, FileAlreadyExistsException, FileNotExistsException {
        StandaloneFileSystemConfiguration configuration = new StandaloneFileSystemConfiguration(XA_DISK_SYSTEM_DIRECTORY, "local");
        XAFileSystem xafs = XAFileSystemProxy.bootNativeXAFileSystem(configuration);
        xafs.waitForBootup(-1);
        Session xaSession = xafs.createSessionForLocalTransaction();

        // Create "tracked" (i.e. non-committed) representations of "dir_A", "dir_B" and "foo.txt"
        xaSession.createFile(new File(CURRENT_WORKING_DIRECTORY, "dir_A/dir_B/foo.txt"), false);

        boolean existResult = xaSession.fileExists(new File(CURRENT_WORKING_DIRECTORY, "dir_A/dir_C/bar.txt"));
        assertThat(existResult, is(false));
    }
}
