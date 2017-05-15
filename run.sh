export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
mvn compile
mvn dependency:build-classpath -Dmdep.outputFile=classpath.out
java -cp ./target/classes:`cat classpath.out` org.linqs.psladmmspark.PSLMaster
