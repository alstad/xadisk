package org.xadisk.filesystem.utilities;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

import static java.io.File.separator;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FileIOUtilityTest {
    private static final String CURRENT_WORKING_DIRECTORY = System.getProperty("user.dir") + separator + "target" + separator + "tmp";

    @Before
    public void testSetup() throws IOException {
        final Path tmpDir = Paths.get(CURRENT_WORKING_DIRECTORY);
        if (Files.exists(tmpDir)) {
            Files.walk(tmpDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    public void deleteDirectoryRecursively_with_files_and_subdirs() throws IOException {
        // Given
        Files.createDirectories(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "bar", "test"));
        Files.createFile(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "data.txt"));
        Files.createFile(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "bar", "other_data.txt"));
        Files.createFile(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "bar", "test", "some_other_data.txt"));

        // When
        FileIOUtility.deleteDirectoryRecursively(new File(CURRENT_WORKING_DIRECTORY, "foo"));

        // Then
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "bar", "test", "some_other_data.txt")), is(false));
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));
    }

    @Test
    public void deleteDirectoryRecursively_with_non_existing_dir() throws IOException {
        // Given
        Files.createDirectories(Paths.get(CURRENT_WORKING_DIRECTORY));
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY)), is(true));
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));

        // When
        FileIOUtility.deleteDirectoryRecursively(new File(CURRENT_WORKING_DIRECTORY, "foo"));

        // Then
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));
    }

    @Test
    public void deleteDirectoryRecursively_with_non_existing_dir_and_parent_dir() throws IOException {
        // Given
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));

        // When
        FileIOUtility.deleteDirectoryRecursively(new File(CURRENT_WORKING_DIRECTORY, "foo"));

        // Then
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));
    }

    @Test
    public void deleteDirectoryRecursively_with_empty_dir() throws IOException {
        // Given
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));
        Files.createDirectories(Paths.get(CURRENT_WORKING_DIRECTORY, "foo"));

        // When
        FileIOUtility.deleteDirectoryRecursively(new File(CURRENT_WORKING_DIRECTORY, "foo"));

        // Then
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));
    }

    @Test(expected = IOException.class)
    public void listDirectoryContents_of_non_existing_dir() throws IOException {
        // Given
        assertThat(Files.exists(Paths.get(CURRENT_WORKING_DIRECTORY, "foo")), is(false));

        // When
        FileIOUtility.listDirectoryContents(Paths.get(CURRENT_WORKING_DIRECTORY, "foo"));
    }

    @Test
    public void listDirectoryContents_on_dir_with_files_and_subdirs() throws IOException {
        // Given
        Files.createDirectories(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "bar", "test"));
        Files.createFile(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "data.txt"));
        Files.createFile(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "bar", "other_data.txt"));
        Files.createFile(Paths.get(CURRENT_WORKING_DIRECTORY, "foo", "bar", "test", "some_other_data.txt"));

        // When
        final String[] dirContent = FileIOUtility.listDirectoryContents(Paths.get(CURRENT_WORKING_DIRECTORY, "foo"));

        // Then
        assertThat(dirContent.length, is(2));
        assertThat(Arrays.asList(dirContent), hasItem("bar"));
        assertThat(Arrays.asList(dirContent), hasItem("data.txt"));

        testSetup();
    }
}
