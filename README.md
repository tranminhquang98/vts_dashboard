# VTS Dashboard
## huymq4@viettel.com.vn && quangtm13@viettel.com.vn

[![N|Solid](https://cldup.com/dTxpPi9lDf.thumb.png)](https://nodesource.com/products/nsolid)

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

## Installation

VTS Dashboard requires [.sbt](https://www.scala-sbt.org/) and [.scala](https://www.scala-lang.org/) to run.

Install the dependencies and start the project

```sh
cd ..
git clone http://10.60.156.11/quangtm13/vts_dashboard.git
```
For production environments...

Add VM option in Intellij

```sh
1. run Application
2. edit configurations...
3. modify option...
 3.1. Paste in VM option:
    -Dlog4j.configuration=file:log4j.properties
    -Dlogfile.name=vts_dashboard
    -Dspark.yarn.app.container.log.dir=app-logs

 3.2. Shorten command line (use JAR manifest)
```
Delete lib

```sh
- File -> Project Structure -> libraries
Delete:
    - logback-classic/1.0.9/logback-classic-1.0.9.jar
    - log4j-slf4j-impl/2.4.1/log4j-slf4j-impl-2.4.1.jar
```

## License

VTS

**Hello world!**
