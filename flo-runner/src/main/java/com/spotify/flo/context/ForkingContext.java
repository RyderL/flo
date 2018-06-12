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
import com.spotify.flo.Task;
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
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
 * <p>
 * The basic idea here is to intercept the process fn by overriding {@link EvalContext#invokeProcessFn(TaskId, Fn)},
 * and executing it in a sub-process JVM. The process fn closure and the result is transported in to and out of the
 * sub-process using serialization.
 */
public class ForkingContext implements EvalContext {

  private static final Logger log = LoggerFactory.getLogger(ForkingContext.class);

  // This is marked transient so as to avoid serializing the whole stack of EvalContexts
  private transient final EvalContext delegate;

  private ForkingContext(EvalContext delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  static EvalContext composeWith(EvalContext baseContext) {
    // Is the Java Debug Wire Protocol activated?
    final boolean inDebugger = ManagementFactory.getRuntimeMXBean().
        getInputArguments().stream().anyMatch(s -> s.contains("-agentlib:jdwp"));

    final Optional<Boolean> forking = Optional.ofNullable(System.getenv("FLO_FORKING")).map(Boolean::parseBoolean);

    if (forking.isPresent()) {
      if (forking.get()) {
        log.debug("Forking enabled (environment variable FLO_FORKING=true)");
        return new ForkingContext(baseContext);
      } else {
        log.debug("Forking disabled (environment variable FLO_FORKING=true)");
        return baseContext;
      }
    } else if (inDebugger) {
      log.debug("Debugger detected, forking disabled by default "
          + "(enable by setting environment variable FLO_FORKING=true)");
      return baseContext;
    } else {
      log.debug("Debugger not detected, forking enabled by default "
          + "(disable by setting environment variable FLO_FORKING=false)");
      return new ForkingContext(baseContext);
    }
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, EvalContext context) {
    return delegate().evaluateInternal(task, context);
  }

  @Override
  public <T> Value<T> value(Fn<T> value) {
    // Note: This method is called from within the process fn lambda, and thus this ForkingContext instance will be
    // captured in the closure. This method will then be called in the task sub-process. As the delegate field is
    // transient it will be null and delegate() will return a SyncContext that will immediately call the value fn.
    return delegate().value(value);
  }

  @Override
  public <T> Value<T> immediateValue(T value) {
    return delegate().immediateValue(value);
  }

  @Override
  public <T> Promise<T> promise() {
    return delegate().promise();
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    if (delegate == null) {
      throw new UnsupportedOperationException("nested execution not supported");
    }
    // Wrap the process fn in a lambda that will execute the original process fn closure in a sub-process.
    final Fn<Value<T>> forkingProcessFn = fork(taskId, processFn);
    // Pass on the wrapped process fn to let the rest of EvalContexts do their thing. The last EvalContext in the chain
    // will invoke the wrapped lambda, causing the sub-process execution to happen there.
    return delegate.invokeProcessFn(taskId, forkingProcessFn);
  }

  private EvalContext delegate() {
    // The delegate field will be null if this is in the sub-process
    if (this.delegate == null) {
      return SyncContext.create();
    } else {
      return delegate;
    }
  }

  private <T> Fn<Value<T>> fork(TaskId taskId, Fn<Value<T>> value) {
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
        processBuilder.environment().put("FLO_TASK_ID", taskId.toString());

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
          log.debug("Subprocess exited with error file");
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
          log.debug("Subprocess exited with result file");
          final T result;
          try {
            result = PersistingContext.deserialize(resultFile);
          } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize result", e);
          }
          return immediateValue(result);
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

    private static class Watchdog extends Thread {

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
          log.error("watchdog failed", e);
        }
        log.debug("child process exiting");
        // Exit with non-zero status code to skip shutdown hooks
        System.exit(-1);
      }
    }

    public static void main(String... args) {
      log.debug("child process started: args={}", Arrays.asList(args));
      final Watchdog watchdog = new Watchdog();
      watchdog.start();

      final TaskId taskId;
      try {
        taskId = TaskId.parse(System.getenv("FLO_TASK_ID"));
      } catch (IllegalArgumentException e) {
        log.error("Failed to read FLO_TASK_ID", e);
        System.exit(2);
        return;
      }
      log.debug("read FLO_TASK_ID = {}", taskId);

      if (args.length != 3) {
        log.error("args.length != 3");
        System.exit(3);
        return;
      }
      final Path closureFile;
      final Path resultFile;
      final Path errorFile;
      try {
        closureFile = Paths.get(args[0]);
        resultFile = Paths.get(args[1]);
        errorFile = Paths.get(args[2]);
      } catch (InvalidPathException e) {
        log.error("Failed to get file path", e);
        System.exit(4);
        return;
      }

      Context.current().withValue(Tracing.TASK_ID, taskId).run(() ->
          run(closureFile, resultFile, errorFile));
    }

    private static void run(Path closureFile, Path resultFile, Path errorFile) {
      log.debug("deserializing closure: {}", closureFile);
      final Fn<Value<?>> fn;
      try {
        fn = PersistingContext.deserialize(closureFile);
      } catch (Exception e) {
        log.error("Failed to deserialize closure: {}", closureFile, e);
        System.exit(5);
        return;
      }

      log.debug("executing closure");
      Value<?> value = null;
      Throwable error = null;
      try {
        value = fn.get();
      } catch (Exception e) {
        error = e;
      }

      log.debug("getting result");
      Object result = null;
      if (value != null) {
        CompletableFuture<Object> f = new CompletableFuture<>();
        value.consume(f::complete);
        value.onFail(f::completeExceptionally);
        try {
          result = f.get();
        } catch (InterruptedException e) {
          error = e;
        } catch (ExecutionException e) {
          error = e.getCause();
        }
      }

      if (error != null) {
        log.debug("serializing error", error);
        try {
          PersistingContext.serialize(error, errorFile);
        } catch (Exception e) {
          log.error("failed to serialize error", e);
          System.exit(6);
          return;
        }
      } else {
        log.debug("serializing result: {}", result);
        try {
          PersistingContext.serialize(result, resultFile);
        } catch (Exception e) {
          log.error("failed to serialize result", e);
          System.exit(7);
          return;
        }
      }

      System.err.flush();
      System.exit(0);
    }
  }
}
