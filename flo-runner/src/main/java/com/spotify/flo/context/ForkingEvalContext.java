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
import com.spotify.flo.TaskId;
import com.spotify.flo.Tracing;
import com.spotify.flo.freezer.PersistingContext;
import io.grpc.Context;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EvalContext} that runs tasks in sub-processes.
 * <p>
 * Forking can be disabled using the environment variable {@code FLO_DISABLE_FORKING=true}.
 * <p>
 * Forking is disabled by default when running in the debugger, but can be enabled by {@code FLO_FORCE_FORK=true}.
 */
class ForkingEvalContext extends ForwardingEvalContext {

  private static final Logger log = LoggerFactory.getLogger(ForwardingEvalContext.class);

  // Is the Java Debug Wire Protocol activated?
  private static boolean IN_DEBUGGER = ManagementFactory.getRuntimeMXBean().
      getInputArguments().stream().anyMatch(s -> s.contains("-agentlib:jdwp"));

  private static boolean FLO_DISABLE_FORKING = Boolean.parseBoolean(System.getenv("FLO_DISABLE_FORKING"));

  private static boolean FORCE_FORK = Boolean.parseBoolean(System.getenv("FLO_FORCE_FORK"));

  private ForkingEvalContext(EvalContext delegate) {
    super(delegate);
  }

  static EvalContext composeWith(EvalContext baseContext) {
    if (FORCE_FORK) {
      log.debug("Forking forcibly enabled (environment variable FORCE_FORK=true)");
      return new ForkingEvalContext(baseContext);
    } else if (FLO_DISABLE_FORKING) {
      log.debug("Forking disabled (environment variable FLO_DISABLE_FORKING=true)");
      return baseContext;
    } else if (IN_DEBUGGER) {
      log.debug("In debugger, forking disabled (enable by setting environment variable FORCE_FORK=true)");
      return baseContext;
    } else {
      return new ForkingEvalContext(baseContext);
    }
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

        log.debug("serializing closure");
        try {
          PersistingContext.serialize(value, closureFile);
        } catch (Exception e) {
          throw new RuntimeException("Failed to serialize closure", e);
        }

        final ProcessBuilder processBuilder = new ProcessBuilder(java.toString(), "-cp", classPath)
            .directory(workdir.toFile());

        // Propagate -Xmx.
        // Note: This is suboptimal because if the user has configured a max heap size we will effectively use that
        // times the concurrent nummber of executing task processes in addition to the heap of the parent process.
        // However, propagating a lower limit might make the task fail if the user has supplied a heap size that is
        // tailored to the memory requirements of the task.
        ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
            .filter(s -> s.startsWith("-Xmx"))
            .forEach(processBuilder.command()::add);

        // Trampoline arguments
        processBuilder.command().add(Trampoline.class.getName());
        processBuilder.command().add(closureFile.toString());
        processBuilder.command().add(resultFile.toString());
        processBuilder.command().add(errorFile.toString());

        // Propagate TASK_ID to child process
        final TaskId taskId = Tracing.TASK_ID.get();
        if (taskId != null) {
          processBuilder.environment().put("FLO_TASK_ID", taskId.toString());
        }

        log.debug("Starting subprocess: environment={}, command={}, directory={}",
            processBuilder.environment(), processBuilder.command(), processBuilder.directory());
        final Process process;
        try {
          process = processBuilder.start();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // Copy std{err,out} line by line to avoid interleaving and corrupting line contents.
        executor.submit(() -> copyLines(process.getInputStream(), System.out));
        executor.submit(() -> copyLines(process.getErrorStream(), System.err));

        log.debug("Waiting for subprocess exit");
        final int exitValue;
        try {
          exitValue = process.waitFor();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } finally {
          process.destroyForcibly();
        }

        log.debug("Subprocess exited: " + exitValue);
        if (exitValue != 0) {
          throw new RuntimeException("Subprocess failed: " + process.exitValue());
        }

        if (Files.exists(errorFile)) {
          // Failed
          log.debug("subprocess exited with error file");
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
          log.debug("subprocess exited with result file");
          final T result;
          try {
            result = PersistingContext.deserialize(resultFile);
          } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize result", e);
          }
          return result;
        }
      } finally {
        executor.shutdown();
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

  private void copyLines(InputStream in, PrintStream out) {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        out.println(line);
      }
    } catch (IOException e) {
      log.error("Caught exception during stream copy", e);
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

      final TaskId taskId;
      try {
        taskId = TaskId.parse(System.getenv("FLO_TASK_ID"));
      } catch (IllegalArgumentException e) {
        try {
          e.printStackTrace();
          System.err.flush();
        } finally {
          System.exit(1);
        }
        return;
      }
      err("read FLO_TASK_ID = " + taskId);

      if (args.length != 3) {
        err("args.length != 3");
        System.exit(1);
        return;
      }
      final Path closureFile = Paths.get(args[0]);
      final Path resultFile = Paths.get(args[1]);
      final Path errorFile = Paths.get(args[2]);

      Context.current().withValue(Tracing.TASK_ID, taskId).run(() ->
          run(closureFile, resultFile, errorFile));
    }

    private static void run(Path closureFile, Path resultFile, Path errorFile) {
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