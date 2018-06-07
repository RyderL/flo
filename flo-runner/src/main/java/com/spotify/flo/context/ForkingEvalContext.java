package com.spotify.flo.context;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.freezer.PersistingContext;
import com.spotify.flo.status.TaskStatusException;
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
                resultFile.toString())
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
          throw new AssertionError("Subprocess failed: " + process.exitValue());
        }

        final T result;
        try {
          result = PersistingContext.deserialize(resultFile);
        } catch (Exception e) {
          throw new RuntimeException("Failed to deserialize result", e);
        }

        if (result instanceof TaskStatusException) {
          throw (TaskStatusException) result;
        }

        return result;

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

      err("deserializing closure");
      final Fn<?> fn;
      try {
        fn = PersistingContext.deserialize(closureFile);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.flush();
        System.exit(1);
        return;
      }

      err("executing closure");
      Object result;
      try {
        result = fn.get();
      } catch (TaskStatusException e) {
        result = e;
      } catch (Exception e) {
        e.printStackTrace();
        System.err.flush();
        System.exit(2);
        return;
      }

      err("serializing result");
      try {
        PersistingContext.serialize(result, resultFile);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.flush();
        System.exit(3);
        return;
      }

      System.err.flush();

      System.exit(0);
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
