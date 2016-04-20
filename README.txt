How to add an external maven project to the build path:

Add the jar to C:\Users\Roland\.m2\repository\%GroupId%\%ArtifactId%\%version%\%ArtifactId%-%version%.jar

Add the dependency into the pom.xml as any remote dependency

How to compile a topology:

cd %project-directory%

set the main class of the topology in the pom.xml file to stormBench.stormBench.MyMainClass

mvn assembly:assembly
