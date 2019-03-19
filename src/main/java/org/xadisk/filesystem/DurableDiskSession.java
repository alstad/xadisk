/*
 Copyright � 2010-2011, Nitin Verma (project owner for XADisk http://xadisk.java.net/). All rights reserved.

 This source code is being made available to the public under the terms specified in the license
 "Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */
package org.xadisk.filesystem;

import org.xadisk.filesystem.utilities.FileIOUtility;
import org.xadisk.filesystem.utilities.MiscUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

public class DurableDiskSession {

    private Set<File> directoriesToForce = new HashSet<File>();
    private boolean synchronizeDirectoryChanges;

    public DurableDiskSession(boolean synchronizeDirectoryChanges) {
        this.synchronizeDirectoryChanges = synchronizeDirectoryChanges;
    }

    public static boolean setupDirectorySynchronization(File xaDiskHome) throws IOException {
        List<String> allParents = new ArrayList<>();
        File parentDirectory = xaDiskHome;
        while (parentDirectory != null) {
            allParents.add(parentDirectory.getAbsolutePath());
            parentDirectory = parentDirectory.getParentFile();
        }
        Collections.reverse(allParents);
        return forceDirectories(allParents.toArray(new String[0]));
    }

    /**
     * Force meta-data for created and changed directories to be flushed/synced to disk.
     *
     * @param directoryPaths paths to directories to sync/flush
     * @return true if sync was successful
     */
    private static boolean forceDirectories(String[] directoryPaths) {
        for (String directoryPath : directoryPaths) {
            try {
                fsync(Paths.get(directoryPath));
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    /**
     * Force any changes (including metadata) to the given file/directory to be
     * written to the storage device that contains it.
     *
     * @param path path to a file or directory
     * @throws IOException If some I/= errors occurs
     */
    public static void fsync(Path path) throws IOException {
        final boolean isDirectory = Files.isDirectory(path);
        OpenOption openOption = isDirectory ? StandardOpenOption.READ : StandardOpenOption.WRITE;
        try(final FileChannel file = FileChannel.open(path, openOption)) {
            file.force(true);
        } catch (IOException ioException) {
            if (isDirectory) {
                return;
            }
            throw ioException;
        }
    }

    public void forceToDisk() throws IOException {
        if (!synchronizeDirectoryChanges) {
            return;
        }
        String paths[] = new String[directoriesToForce.size()];
        int i = 0;
        for (File dir : directoriesToForce) {
            paths[i++] = dir.getAbsolutePath();
        }
        if (!forceDirectories(paths)) {
            throw new IOException("Fatal Error: Directory changes could not be forced-to-disk during transaction commit.");
        }
    }

    private void forceToDisk(String directory) throws IOException {
        if (!synchronizeDirectoryChanges) {
            return;
        }
        String paths[] = new String[1];
        paths[0] = directory;
        if (!forceDirectories(paths)) {
            throw new IOException("Fatal Error: Directory changes could not be forced-to-disk during transaction commit.");
        }
    }

    public void renameTo(File src, File dest) throws IOException {
        directoriesToForce.add(src.getParentFile());
        directoriesToForce.add(dest.getParentFile());
        if (directoriesToForce.remove(src)) {
            directoriesToForce.add(dest);
        }
        updatePathsForDescendantDirectories(src, dest);
        FileIOUtility.renameTo(src, dest);
    }

    private void updatePathsForDescendantDirectories(File ancestorOldPath, File ancestorNewPath) {
        File dirs[] = directoriesToForce.toArray(new File[0]);
        for (File dirName : dirs) {
            ArrayList<String> stepsToDescendToDir = MiscUtils.isDescedantOf(dirName, ancestorOldPath);
            if (stepsToDescendToDir != null) {
                StringBuilder newPathForDir = new StringBuilder(ancestorNewPath.getAbsolutePath());
                for (int j = stepsToDescendToDir.size() - 1; j >= 0; j--) {
                    newPathForDir.append(File.separator).append(stepsToDescendToDir.get(j));
                }
                directoriesToForce.remove(dirName);
                directoriesToForce.add(new File(newPathForDir.toString()));
            }
        }
    }

    public void deleteFile(File f) throws IOException {
        directoriesToForce.add(f.getParentFile());
        directoriesToForce.remove(f);
        FileIOUtility.deleteFile(f);
    }

    public void deleteFileDurably(File file) throws IOException {
        FileIOUtility.deleteFile(file);
        forceToDisk(file.getParentFile().getAbsolutePath());
    }

    public void createFile(File f) throws IOException {
        directoriesToForce.add(f.getParentFile());
        FileIOUtility.createFile(f);
    }

    public void createFileDurably(File file) throws IOException {
        FileIOUtility.createFile(file);
        forceToDisk(file.getParentFile().getAbsolutePath());
    }

    public void createDirectory(File dir) throws IOException {
        directoriesToForce.add(dir.getParentFile());
        FileIOUtility.createDirectory(dir);
    }

    public void createDirectoryDurably(File dir) throws IOException {
        FileIOUtility.createDirectory(dir);
        forceToDisk(dir.getParentFile().getAbsolutePath());
    }

    public void deleteDirectoryRecursively(File dir) throws IOException {
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                deleteDirectoryRecursively(files[i]);
            } else {
                deleteFile(files[i]);
            }
        }
        deleteEmptyDirectory(dir);
    }

    public void createDirectoriesIfRequired(File dir) throws IOException {
        if (dir.isDirectory()) {
            return;
        }
        createDirectoriesIfRequired(dir.getParentFile());
        createDirectory(dir);
    }

    private void deleteEmptyDirectory(File dir) throws IOException {
        deleteFile(dir);
    }
}
