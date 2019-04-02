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
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;

import org.xadisk.filesystem.NativeXAFileSystem;

public class FileIOUtility {

    public static void renameTo(File src, File dest) throws IOException {
        Files.move(src.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    public static void deleteFile(File f) throws IOException {
        Files.delete(f.toPath());
    }

    public static void deleteDirectoryRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void createFile(File f) throws IOException {
        Files.createFile(f.toPath());
    }

    public static void createDirectory(File dir) throws IOException {
        Files.createDirectory(dir.toPath());
    }

    public static void createDirectoriesIfRequired(File dir) throws IOException {
        if (dir.isDirectory()) {
            return;
        }
        Files.createDirectories(dir.toPath());
    }

    public static String[] listDirectoryContents(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files
                    .map(Path::toFile)
                    .map(File::getName)
                    .toArray(String[]::new);
        }
    }

    public static void readFromChannel(FileChannel fc, ByteBuffer buffer, int bufferOffset, int num)
            throws IOException {
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
