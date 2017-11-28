# BenchMark

### Result

| Framework          | SQL  | TableAPI | Tuple | Row  | POJO |
| ------------------ | ---- | -------- | ----- | ---- | ---- |
| Flink              |      |          |       |      |      |
| Spark              |      |          |       |      |      |
| Flink + Expression |      |          |       | 48s  |      |



### Cheatsheet

mvn install:install-file -Dfile=/Users/dchen/Downloads/ExpressionImplDependencies-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=ExpressionImplDependencies -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true