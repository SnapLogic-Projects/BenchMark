# BenchMark

### Result

| Framework          | SQL  | TableAPI | Tuple | Row       | POJO | Document |
| ------------------ | ---- | -------- | ----- | --------- | ---- | -------- |
| Flink              | 21s  | 16s      | 19s   | 20s       | 21s  | 27s      |
| Spark              | 19s  |          |       |           | 10s  |          |
| Flink + Expression |      |          |       | 26s (20s) |      | 30s      |



### Cheatsheet

mvn install:install-file -Dfile=/Users/dchen/Downloads/ExpressionImplDependencies-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=ExpressionImplDependencies -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true