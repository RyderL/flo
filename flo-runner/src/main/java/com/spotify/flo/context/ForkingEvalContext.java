/*-
 * -\-\-
 * Flo Runner
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.context;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.freezer.PersistingContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForkingEvalContext extends ForwardingEvalContext {

  private static final Logger log = LoggerFactory.getLogger(ForwardingEvalContext.class);

  public ForkingEvalContext(EvalContext delegate) {
    super(delegate);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new ForkingEvalContext(baseContext);
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    return super.value(fork(value));
  }

  private <T> Fn<T> fork(Fn<T> value) {
    return () -> {

      final ExecutorService executor = Executors.newCachedThreadPool();
      Path tempdir = null;

      try {
        final Path workdir;
        try {
          tempdir = Files.createTempDirectory("flo-fork");
          workdir = Files.createDirectory(tempdir.resolve("workdir"));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        final Path closureFile = tempdir.resolve("closure");
        final Path resultFile = tempdir.resolve("result");
        final Path errorFile = tempdir.resolve("error");

        final String home = System.getProperty("java.home");
        final String classPath = System.getProperty("java.class.path");
        final Path java = Paths.get(home, "bin", "java").toAbsolutePath().normalize();

        try {
          PersistingContext.serialize(value, closureFile);
        } catch (Exception e) {
          throw new RuntimeException("Failed to serialize closure", e);
        }

        final ProcessBuilder processBuilder = new ProcessBuilder()
            .command(
                java.toString(),
                "-cp", classPath,
                Trampoline.class.getName(),
                closureFile.toString(),
                resultFile.toString(),
                errorFile.toString())
            .directory(workdir.toFile());

        final Process process;
        try {
          process = processBuilder.start();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // TODO: log output
        // TODO: propagate grpc context
        executor.submit(() -> copy(process.getInputStream(), System.out));
        executor.submit(() -> copy(process.getErrorStream(), System.err));

        final boolean exited;
        try {
          // TODO: configurable timeout
          exited = process.waitFor(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
          process.destroyForcibly();
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        if (!exited) {
          process.destroyForcibly();
          throw new RuntimeException("Subprocess timed out");
        }

        if (process.exitValue() != 0) {
          throw new RuntimeException("Subprocess failed: " + process.exitValue());
        }

        if (Files.exists(errorFile)) {
          // Failed
          final Throwable error;
          try {
            error = PersistingContext.deserialize(errorFile);
          } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize error", e);
          }
          if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
          } else {
            throw new RuntimeException(error);
          }
        } else {
          // Success
          final T result;
          try {
            result = PersistingContext.deserialize(resultFile);
          } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize result", e);
          }
          return result;
        }
      } finally {
        executor.shutdownNow();
        tryDeleteDir(tempdir);
      }
    };
  }

  private void tryDeleteDir(Path path) {
    try {
      deleteDir(path);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory: {}", path, e);
    }
  }

  private void deleteDir(Path path) throws IOException {
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
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

  private void copy(InputStream in, OutputStream out) {
    byte[] buffer = new byte[1024];
    try {
      while (true) {
        int r = in.read(buffer);
        if (r < 0) {
          break;
        }
        out.write(buffer, 0, r);
        out.flush();
      }
    } catch (IOException e) {
      log.error("Caught exception during byte stream copy", e);
    }
  }

  @SuppressWarnings("finally")
  private static class Trampoline {

    private static final boolean DEBUG = Boolean.parseBoolean(System.getenv("FLO_DEBUG_FORKING"));

    private static final String NAME = ManagementFactory.getRuntimeMXBean().getName();

    private static class Watchdog
        extends Thread {

      Watchdog() {
        setDaemon(false);
      }

      @Override
      public void run() {
        // Wait for parent to exit.
        try {
          while (true) {
            int c = System.in.read();
            if (c == -1) {
              break;
            }
          }
        } catch (IOException e) {
          errPrefix();
          e.printStackTrace(System.err);
          System.err.flush();
        }
        System.err.println();
        err("child process exiting");
        // Exit with non-zero status code to skip shutdown hooks
        System.exit(-1);
      }
    }

    public static void main(String... args) {
      err("child process started");
      Watchdog watchdog = new Watchdog();
      watchdog.start();

      final Path closureFile = Paths.get(args[0]);
      final Path resultFile = Paths.get(args[1]);
      final Path errorFile = Paths.get(args[2]);

      err("deserializing closure");
      final Fn<?> fn;
      try {
        fn = PersistingContext.deserialize(closureFile);
      } catch (Exception e) {
        try {
          e.printStackTrace();
          System.err.flush();
        } finally {
          System.exit(1);
        }
        return;
      }

      err("executing closure");
      Object result = null;
      Throwable error = null;
      try {
        result = fn.get();
      } catch (Exception e) {
        error = e;
      }

      if (error != null) {
        err("serializing error");
        try {
          PersistingContext.serialize(error, errorFile);
        } catch (Exception e) {
          try {
            err("failed to serialize error");
            error.printStackTrace();
            err("===============");
            e.printStackTrace();
            err("===============");
            System.err.flush();
          } finally {
            System.exit(2);
          }
          return;
        }
      } else {
        err("serializing result");
        try {
          PersistingContext.serialize(result, resultFile);
        } catch (Exception e) {
          try {
            err("failed to serialize result");
            e.printStackTrace();
            System.err.flush();
          } finally {
            System.exit(3);
          }
          return;
        }
      }

      try {
        System.err.flush();
      } finally {
        System.exit(0);
      }
    }

    private static void err(String message) {
      if (DEBUG) {
        errPrefix();
        System.err.println(message);
        System.err.flush();
      }
    }

    private static void errPrefix() {
      System.err.print(LocalTime.now() + " [" + NAME + "] " + ForkingEvalContext.class.getName() + ": ");
    }
  }
}
