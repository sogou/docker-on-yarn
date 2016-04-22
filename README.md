# Docker on yarn - Unified Resource Management Submit System

---

Docker ont yarn is a simply system to submit your hadoop job.

---

## User Guide

### Maven Dependency

```
<dependency>
  <groupId>distributed-docker-submit-project</groupId>
  <artifactId>distributed-docker-submit-project</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```

---

## Developer Guide

### Requirements

* JDK-1.7
* Maven 3.0.3 above

### Building

```
$ mvn clean package
```

* jar location: distributed-docker-appmaster/target/,distributed-docker-submit-client-0.0.1-SNAPSHOT.jar

### Deploy To Remote Maven Repository

```
$ mvn deploy
```

---
