package io.rouz.task;

import com.google.auto.value.AutoValue;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.dsl.TaskBuilder.F0;
import io.rouz.task.dsl.TaskBuilder.F1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * TODO:
 * d make inputs lazily instantiated to allow short circuiting graph generation
 * . output task graph
 * . external outputs - outputs that might already be available (ie a file on disk)
 *   - can be implemented with a regular task, but better support can be added
 * . id task
 *
 * {@code Task<T>(param1=value1, param2=valu2) => t}
 *
 * @param <T>  A type carrying the execution metadata of this task
 */
@AutoValue
public abstract class Task<T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Task.class);

  public abstract TaskId id();

  abstract Stream<? extends Task<?>> inputs();
  abstract F1<TaskContext, T> code();

  public Stream<TaskId> tasksInOrder() {
    return tasksInOrder(new LinkedHashSet<>());
  }

  // fixme: this can only be called once per task instance because it operates on the inputs stream
  protected Stream<TaskId> tasksInOrder(Set<TaskId> visits) {
    return inputs()
        .filter(task -> !visits.contains(task.id()))
        .flatMap(
            task -> {
              visits.add(task.id());
              return Stream.concat(
                  task.tasksInOrder(visits),
                  Stream.of(task.id()));
            });
  }

  public T out() {
    return code().apply(new TaskContextImpl());
  }

  T internalOut(TaskContext taskContext) {
    return code().apply(taskContext);
  }

  public static TaskBuilder named(String taskName, Object... args) {
    return TaskBuilders.rootBuilder(taskName, args);
  }

  public static <T> Task<T> create(F0<T> code, String taskName, Object... args) {
    return create(Stream.empty(), tc -> code.get(), taskName, args);
  }

  /**
   * Used to resolve a serialization proxy.
   *
   * See {@link NonStreamTask}
   *
   * @return a serializable proxy for this task
   * @throws ObjectStreamException
   */
  protected Object writeReplace() throws ObjectStreamException {
    return NonStreamTask.create(id(), inputs().collect(toList()), code());
  }

  static <T> Task<T> create(
      Stream<? extends Task<?>> inputs,
      F1<TaskContext, T> code,
      String taskName,
      Object... args) {
    final TaskId taskId = TaskIds.create(taskName, args);
    return new AutoValue_Task<>(taskId, inputs, memoize(taskId, code));
  }

  private static <T> F1<TaskContext, T> memoize(TaskId taskId, F1<TaskContext, T> fn) {
    return taskContext -> {
      if (taskContext.has(taskId)) {
        final T value = taskContext.value(taskId);
        LOG.debug("Found calculated value for {} = {}", taskId, value);
        return value;
      } else {
        final T value = fn.apply(taskContext);
        taskContext.put(taskId, value);
        return value;
      }
    };
  }
}
