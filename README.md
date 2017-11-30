# BenchMark

### Result
#### 800MB

| Framework          | SQL  | TableAPI | Tuple | Row       | POJO | Document |
| ------------------ | ---- | -------- | ----- | --------- | ---- | -------- |
| Flink              | 21s  | 16s      | 19s   | 20s       | 21s  | 27s      |
| Spark              | 19s  |          |       |           | 10s  |          |
| Flink + Expression |      |          |       | 26s (20s) |      | 30s      |

#### 8GB
| Framework          | SQL  | TableAPI | Tuple | Row       | POJO | Document |
| ------------------ | ---- | -------- | ----- | --------- | ---- | -------- |
| Flink              | 1m8s | 16s      | 1m6s  | 1m11s     | 1m16s| 1m49s    |
| Spark              | 2m42s|          |       |           | 1m18s|          |
| Flink + Expression |      |          |       | 1m52s     |      | 2m45s    |



### Cheatsheet

mvn install:install-file -Dfile=/Users/dchen/Downloads/ExpressionImplDependencies-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=ExpressionImplDependencies -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true
