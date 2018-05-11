# Develop Note

## Dependency
 - JDK 1.7
 - Maven

## __IDE__

  Can run on Java IDE that supports maven. 

## __Run__

  Startup class is `DMDSStartup.java`.
  
  If you do not want to use the default configuration in the resources directory, you can configure the environment variable `DMDS_HOME`, then put configuration files under`${DMDS_HOME}/conf`.

## __License header__

   You can copy and paste the license header as comment from below.
  
  ```
    Copyright (C) 2016-2020 zhongan.com
    License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
  ```
## __Test__

  Make sure all the test cases are passed, Make sure` mvn clean install` can be compiled and tested successfully.
    
## __Code style__

   See  [intellij-java-google-style](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml)

## Commit message style

  ```
    <issue id><whitesapce><issue title>
    <how to do>
  ```
