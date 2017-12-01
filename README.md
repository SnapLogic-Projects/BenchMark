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
| Flink              | 1m8s |          | 1m6s  | 1m11s     | 1m16s| 1m49s    |
| Spark              | 2m42s|          |       |           | 1m18s|          |
| Flink + Expression |      |          |       | 1m52s     |      | 2m45s    |

SnapLogic: 3m31s - 3m

### Cheatsheet

mvn install:install-file -Dfile=/Users/dchen/Downloads/ExpressionImplDependencies-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=ExpressionImplDependencies -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true

### Setup
1. mvn install:install-file -Dfile=/Users/benson/Dropbox/Snaplogic/Projects/Unify/git/Snap-document/DocumentImpl/target/DocumentImpl-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=DocumentImpl -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true
2. mvn install:install-file -Dfile=/Users/benson/Dropbox/Snaplogic/Projects/Unify/git/Snap-document/ExpressionImplDependencies/target/ExpressionImplDependencies-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=ExpressionImplDependencies -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true
3. mvn install:install-file -Dfile=/Users/benson/Dropbox/Snaplogic/Projects/Unify/git/Snap-document/ExpressionImpl/target/ExpressionImpl-1.0-SNAPSHOT.jar -DgroupId=com.snaplogic -DartifactId=ExpressionImpl -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DgenereatePom=true

### Execute
`bin/flink run -c com.snaplogic.benchmark.DataSetAPIBenchmark /Users/benson/Dropbox/Snaplogic/Projects/Unify/git/BenchMark/target/quickstart-0.1-flinkdemo-fat-jar.jar ~/Downloads/test_8GB.csv ~/Downloads/result.csv`
