# DMDS

__DMDS__(Distributed MySQL Database Service) is a MySQL sharding middleware  base on [MyCAT](https://github.com/MyCATApache/Mycat-Server).

## What is Different ?

- __Table sharding__ 

  Support sharding database and table in the same time.

- __Simplify__

  Focus on MySQL ,  remove some unnecessary features like zookeeper config 、 catlte .

## Deploy

- __Package__

  After download source code, package it with maven and JDK (verison >= 7). 

  ```bash
  mvn clean package -DskipTests=true
  ```

- __Start__

  Copy package `dmds-server-xxx.tar` to deploy directory. Run `bin/startup.sh ` after unzip it.

  ```bash
  cp target/dmds-server-xxx.tar.gz deploy/
  cd deploy/
  tar -xf dmds-server-xxx.tar.gz
  cd dmds-server-1.0.0/
  ./bin/startup.sh
  ```

- __Stop__

  Run `bin/stop.sh` to stop it.

## Sharding Configuration Example

  Configuration files are under conf directory like MyCAT. Let's see an example for 8 sharding tables in 2 databases.

- __schema.xml__

  Configure hostNode、dataNode and schema. Schema name is `dmds_test` , data node is `dn_dmds_test_00` and `dn_dmds_test_01`

  ```xml
      <schema name="dmds_test" checkSQLschema="true" sqlMaxLimit="5000" dataNode="dn_dmds_test_00">
          <table name="ts_order" primaryKey="id" dataNode="dn_dmds_test_00, dn_dmds_test_01"
                 rule="rule_dmds_test_ts_order"/>
      </schema>
      <dataNode name="dn_dmds_test_00" dataHost="dh_dmds_test_00" database="dmds_test_00"/>
      <dataNode name="dn_dmds_test_01" dataHost="dh_dmds_test_00" database="dmds_test_01"/>
      <dataHost name="dh_dmds_test_00" maxCon="100" minCon="1" balance="0" writeType="0" dbType="mysql"
                dbDriver="native" switchType="1" slaveThreshold="100">
          <heartbeat>select 1</heartbeat>
          <writeHost host="M-dh_dmds_test_00" url="localhost:3306" user="dmds_test_user" password="pwd135"/>
      </dataHost>
  ```

- __rule.xml__

  Configure tableRule and sharding function. Function class must be `com.zhongan.dmds.route.function.PartitionTbByMod`. 

  - shardColumnType

     Data type of sharding column, `0` for numerical  and `1` for string.

  ```xml
  <tableRule name="rule_dmds_test_ts_order">
      <rule>
          <columns>id</columns>
          <algorithm>function_dmds_test_ts_order</algorithm>
      </rule>
  </tableRule>
  <function name="function_dmds_test_ts_order"
            class="com.zhongan.dmds.route.function.PartitionTbByMod">
      <property name="dbCount">2</property>
      <property name="tbCount">4</property>
      <property name="shardColumnType">0</property>
  </function>
  ```

- __server.xml__

  Configure access user. user name is `dmds_test`

  ```xml
      <user name="dmds_test">
          <property name="password">4fe313bd</property>
          <property name="schemas">dmds_test</property>
      </user>
  ```

- __Table distributed__

  The physical table is distributed in order on 2 databases. Table names are automatically generated.

  ```sql
  select TABLE_SCHEMA, TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA like 'dmds_test_%' and TABLE_NAME like 'ts_order_%';
  +--------------+---------------+
  | TABLE_SCHEMA | TABLE_NAME    |
  +--------------+---------------+
  | dmds_test_00 | ts_order_0000 |
  | dmds_test_00 | ts_order_0001 |
  | dmds_test_00 | ts_order_0002 |
  | dmds_test_00 | ts_order_0003 |
  | dmds_test_01 | ts_order_0004 |
  | dmds_test_01 | ts_order_0005 |
  | dmds_test_01 | ts_order_0006 |
  | dmds_test_01 | ts_order_0007 |
  +--------------+---------------+
  ```

## Contributing 

  See [Contributing to DMDS](CONTRIBUTING.md) for details on reporting bugs and commit pull request.

## License

  DMDS are licensed under [GPL2.0 license](https://www.gnu.org/licenses/old-licenses/lgpl-2.0.html)

## More Information
  For more information, please see [MyCAT documents](https://github.com/MyCATApache/Mycat-doc/tree/master/en).