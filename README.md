# BenchMark

### Result

| Framework          | SQL  | TableAPI | Tuple | Row  | POJO | Document |
| ------------------ | ---- | -------- | ----- | ---- | ---- | -------- |
| Flink              |      | 37s      | 54s   | 45s  | 54s  | 1m10s    |
| Spark              |      |          |       | \    |      |          |
| Flink + Expression |      |          |       | 48s  |      |          |



### Cheatsheet

mvn install:install-file -Dfile=/Users/dchen/Downloads/ExpressionImplDependencies-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=ExpressionImplDependencies -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true