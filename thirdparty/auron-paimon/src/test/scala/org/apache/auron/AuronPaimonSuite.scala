/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron

import org.apache.spark.sql.AuronQueryTest

class AuronPaimonSuite extends AuronQueryTest with BaseAuronPaimonSQLSuite {

  test("test paimon with partition table error") {
    withTable("t1") {
      sql(
        "create table t1 (id string) " +
          "ROW FORMAT SERDE 'org.apache.paimon.hive.PaimonSerDe' " +
          "STORED AS " +
          "INPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonInputFormat' " +
          "OUTPUTFORMAT 'org.apache.paimon.hive.mapred.PaimonOutputFormat'")
      sql("desc table formatted t1").show()
      sql("insert into t1 values('1')")
      val df = sql("select * from t1 where pt = '2026-03-10'")
      df.show()
      assert(df.collect().toList.head.get(0) == "1")
      assert(df.collect().toList.head.get(1) == "2026-03-10")
    }
  }

}
