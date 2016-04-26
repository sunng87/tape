package com.squareup.tape;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by nsun on 16-4-25.
 */
public class InfQueueTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    InfQueue<String> q;

    @Before
    public void setUp() throws IOException {
        File parent = folder.getRoot();
        File file = new File(parent, "infqueue-file");
        q = new InfQueue<String>(1, new SerializedConverter<String>(), file);
    }

    @Test
    public void testPut() throws IOException, InterruptedException {
        q.put("hello");
        q.put("world");

        assertThat(q.size()).isEqualTo(2);
        q.close();
    }

    @Test
    public void testOffer() throws IOException {
        assertThat(q.offer("hello")).isTrue();
        assertThat(q.offer("world")).isTrue();
        assertThat(q.size()).isEqualTo(2);
    }

    @Test
    public void testOfferWithTimeout() throws InterruptedException {
        assertThat(q.offer("hello", 100, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(q.offer("world", 100, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(q.size()).isEqualTo(2);
    }

    @Test
    public void testTake() throws InterruptedException {
        q.put("hello");
        q.put("world");

        assertThat(q.take()).isEqualTo("world");
        assertThat(q.take()).isEqualTo("hello");
    }

    @Test
    public void testPoll() throws InterruptedException {
        q.put("hello");
        q.put("world");
        assertThat(q.poll()).isEqualTo("world");
        assertThat(q.poll()).isEqualTo("hello");
        assertThat(q.poll()).isNull();
    }

    @Test
    public void testPollWithTimeout() throws InterruptedException {
        q.put("hello");
        q.put("world");

        assertThat(q.poll(100, TimeUnit.MILLISECONDS)).isEqualTo("world");
        assertThat(q.poll(100, TimeUnit.MILLISECONDS)).isEqualTo("hello");
        assertThat(q.poll(100, TimeUnit.MILLISECONDS)).isNull();
    }
}
