# flo

> __Dag__ means __Day__ in Swedish

```java
class Fib {

  @RootTask
  static Task<Long> nth(int n) {
    TaskBuilder fib = Task.named("Fib", n);
    if (n < 3) {
      return fib
          .process(() -> 1L);
    } else {
      return fib
          .in(() -> Fib.nth(n - 1))
          .in(() -> Fib.nth(n - 2))
          .process(Fib::plus);
    }
  }

  static long plus(long a, long b) {
    return a + b;
  }

  public static void main(String[] args) throws IOException {
    Cli.forFactories(FloRootTaskFactory.Fib_Nth()).run(args);
  }
}
```

```
$ java -jar fib.jar list
available tasks:

Fib.nth
```

```
$ java -jar fib.jar create Fib.nth -h
Option (* = required)  Description
---------------------  -----------
-h, --help
* -n <Integer: n>
```

```
$ time java -jar fib.jar create Fib.nth -n 92

task.id() = Fib(92)#7178b126
task.out() = 7540113804746346429
0.49s user 0.05s system 178% cpu 0.304 total
```
