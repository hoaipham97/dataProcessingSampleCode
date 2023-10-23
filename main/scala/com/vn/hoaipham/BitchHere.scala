package com.vn.hoaipham
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object RetargetingTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val spark_session = SparkSession.builder().master("local[*]").appName("Spark Test Retargeting").getOrCreate()
    val path_installs = "/Users/hoaipham/Desktop/my_script/spark_tut/src/data/retargeting_data_collection_mapping_install/*.parquet"
    val df = spark_session.read.parquet(path_installs)
    df.createOrReplaceTempView("retargeting_data_collection_mapping_install")
    val check1 = f"""
                    |SELECT distinct App_ID
                    |FROM retargeting_data_collection_mapping_install
                    |""".stripMargin
    spark_session.sql(check1).show(50)


//    val retargeting_cohort_campaign_unified_by_ua_install_sql =
//      f"""
//         |SELECT
//         |Cohort_Date,
//         |User_Type,
//         |App_ID,
//         |Country_Code,
//         |Media_Source_UA,
//         |Media_Source_RE,
//         |Campaign_ID_UA,
//         |Campaign_ID_RE,
//         |COUNT(DISTINCT(CASE WHEN Cycle_Day between 0 and 180 THEN AppsFlyer_ID ELSE null END)) as total_paying_users,
//         |SUM(CASE WHEN Cycle_Day < 1 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_0_gross,
//         |SUM(CASE WHEN Cycle_Day < 2 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_1_gross,
//         |SUM(CASE WHEN Cycle_Day < 3 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_2_gross,
//         |SUM(CASE WHEN Cycle_Day < 4 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_3_gross,
//         |SUM(CASE WHEN Cycle_Day < 8 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_7_gross,
//         |SUM(CASE WHEN Cycle_Day < 15 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_14_gross,
//         |SUM(CASE WHEN Cycle_Day < 31 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_30_gross,
//         |SUM(CASE WHEN Cycle_Day < 46 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_45_gross,
//         |SUM(CASE WHEN Cycle_Day < 61 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_60_gross,
//         |SUM(CASE WHEN Cycle_Day < 91 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_90_gross,
//         |SUM(CASE WHEN Cycle_Day < 121 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_120_gross,
//         |SUM(CASE WHEN Cycle_Day < 151 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_150_gross,
//         |SUM(CASE WHEN Cycle_Day < 181 THEN Event_Revenue_USD ELSE 0 END) AS af_purchase_sum_day_180_gross,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 1 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_0,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 2 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_1,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 3 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_2,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 4 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_3,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 8 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_7,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 15 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_14,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 31 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_30,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 46 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_45,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 61 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_60,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 91 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_90,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 121 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_120,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 151 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_150,
//         |COUNT(DISTINCT (CASE WHEN Cycle_Day < 181 THEN AppsFlyer_ID ELSE NULL END)) AS af_purchase_unique_users_day_180
//         |FROM user_level_rev_cohort
//         |GROUP BY 1,2,3,4,5,6,7,8
//         |ORDER BY 1
//         |""".stripMargin
////    val retargeting_cohort_campaign_unified_by_ua_install = spark_session.sql(retargeting_cohort_campaign_unified_by_ua_install_sql)
////    retargeting_cohort_campaign_unified_by_ua_install.write.mode("overwrite").parquet("/Users/hoaipham/Desktop/my_script/spark_tut/src/data/retargeting_cohort_campaign_unified_by_ua_install")
////    retargeting_cohort_campaign_unified_by_ua_install.createOrReplaceTempView("retargeting_cohort_campaign_unified_by_ua_install")
//
////    spark_session.sql(retargeting_cohort_campaign_unified_by_ua_install).createOrReplaceTempView("retargeting_cohort_campaign_unified_by_ua_install")

  }

}

