# Logging

## Overriding log levels at launch

By default logging level is set to `INFO`.
You can override the defaults without changing the packaged file by passing JVM system properties:

- `-DLOG_LEVEL_ROOT=...` – controls the root logger level
- `-DLOG_LEVEL_JGIT=...` – controls `org.eclipse.jgit.*`
- `-DLOG_LEVEL_GHS=...` – controls `com.gerritforge.*`

## Eexamples

### Run with defaults (INFO level)

```bash
java -jar bazel-bin/plugins/ghs-actions-executor/ghs-actions-executor.jar \\
  -v PreserveOutdatedBitmapsAction /path/to/repo.git
```

### Enable debug for application code only

Keep root INFO, but increase only `com.gerritforge` to DEBUG:

```bash
java -DLOG_LEVEL_GHS=DEBUG \\
  -jar bazel-bin/plugins/ghs-actions-executor/ghs-actions-executor.jar \\
  -v PreserveOutdatedBitmapsAction /path/to/repo.git
```
