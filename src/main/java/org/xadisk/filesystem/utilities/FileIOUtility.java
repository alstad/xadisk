/*
 Copyright ? 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

 This source code is being made available to the public under the terms specified in the license
 "Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */
package org.xadisk.filesystem.utilities;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.xadisk.filesystem.NativeXAFileSystem;

public class FileIOUtility {

    public static void renameTo(File src, File dest) throws IOException {
        Files.move(src.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    public static void deleteFile(File f) throws IOException {
        Files.delete(f.toPath());
    }

    private static void deleteEmptyDirectory(File dir) throws IOException {
        deleteFile(dir);
    }

    public static void deleteDirectoryRecursively(File dir) throws IOException {
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

    public static void createFile(File f) throws IOException {
        if (f.createNewFile()) {
            return;
        }
        if (f.exists()) {
            throw new IOException("File already exists.");
        }
        if (!f.getParentFile().canWrite()) {
            throw new IOException("Parent directory not writable.");
        }
        int retryCount = 1;
        while (!f.createNewFile()) {
            doGCBeforeRetry(retryCount++, f);
        }
    }

    public static void createDirectory(File dir) throws IOException {
        if (dir.mkdir()) {
            return;
        }
        if (dir.exists()) {
            throw new IOException("File already exists.");
        }
        if (!dir.getParentFile().canWrite()) {
            throw new IOException("Parent directory not writable.");
        }
        int retryCount = 1;
        while (!dir.mkdir()) {
            doGCBeforeRetry(retryCount++, dir);
        }
    }

    public static void createDirectoriesIfRequired(File dir) throws IOException {
        if (dir.isDirectory()) {
            return;
        }
        Files.createDirectories(dir.toPath());
    }

    public static String[] listDirectoryContents(File dir) throws IOException {
        if (!dir.isDirectory()) {
            throw new IOException("The directory doesn't exist.");
        }
        if (!dir.canRead()) {
            throw new IOException("The directory is not readable.");
        }
        String children[] = dir.list();
        int retryCount = 1;
        while (children == null) {
            doGCBeforeRetry(retryCount++, dir);
            children = dir.list();
        }
        return children;
    }

    private static void doGCBeforeRetry(int retryCount, File f) throws IOException {
        if (retryCount == 5) {
            throw new IOException("The i/o operation could not be completed for "
                    + "the file/directory with path [" + f.getAbsolutePath() + "] due "
                    + "to an unknown reason.");
        }

        System.gc();
        System.gc();
        System.gc();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw (IOException) new IOException().initCause(ie);
        }
    }

    public static void readFromChannel(FileChannel fc, ByteBuffer buffer, int bufferOffset, int num)
            throws IOException, EOFException {
        buffer.position(bufferOffset);
        if (buffer.remaining() < num) {
            throw new BufferUnderflowException();
        }
        buffer.limit(bufferOffset + num);
        int numRead = 0;
        int t = 0;
        while (numRead < num) {
            t = fc.read(buffer);
            if (t == -1) {
                throw new EOFException();
            }
            numRead += t;
        }
    }

    public static void copyFile(File src, File dest, boolean append) throws IOException {
        FileInputStream srcFileInputStream = null;
        FileOutputStream destFileOutputStream = null;
        try {
            srcFileInputStream = new FileInputStream(src);
            destFileOutputStream = new FileOutputStream(dest, append);
            FileChannel srcChannel = srcFileInputStream.getChannel();
            FileChannel destChannel = destFileOutputStream.getChannel();
            long contentLength = srcChannel.size();
            long num = 0;
            while (num < contentLength) {
                num += srcChannel.transferTo(num, NativeXAFileSystem.maxTransferToChannel(contentLength - num), destChannel);
            }

            destChannel.force(false);
        } finally {
            MiscUtils.closeAll(srcFileInputStream, destFileOutputStream);
        }
    }

    public static void copyFile(InputStream srcStream, File dest, boolean append) throws IOException {
        FileOutputStream destStream = null;
        try {
            destStream = new FileOutputStream(dest, append);
            byte[] b = new byte[1000];
            int numRead = 0;
            while (true) {
                numRead = srcStream.read(b);
                if (numRead == -1) {
                    break;
                }
                destStream.write(b, 0, numRead);
            }

            destStream.flush();
        } finally {
            MiscUtils.closeAll(srcStream, destStream);
        }
    }
}
