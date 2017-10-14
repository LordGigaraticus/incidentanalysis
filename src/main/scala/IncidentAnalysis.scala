
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}
import scalikejdbc.{AutoSession, ConnectionPool, _}

/**
  * Created by dakotahrickert on 7/21/17.
  */
object IncidentAnalysis {

  //Properties File Data
  //---------------------------------------------
  val prop = new Properties()
  prop.load(ClassLoader.getSystemResourceAsStream("project.properties"))

  val DB_PATH = prop.getProperty("DATABASE_PATH")
  val USERNAME = prop.getProperty("USERNAME")
  val PASSWORD = prop.getProperty("PASSWORD")
  //---------------------------------------------

  //Suppress Spark Logging
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  //Create Spark Session
  val spark = SparkSession.builder().appName("IncidentAnalysis").master("local[*]").getOrCreate()
  //Import Implicits
  import spark.implicits._


  //Setup Connection to Postgres
  implicit val session = AutoSession
  ConnectionPool.singleton(DB_PATH, USERNAME, PASSWORD)


  //Main function for testing/running functions
  //---------------------------------------------
  def main(args: Array[String]): Unit = {
    updateServiceRequests()
    updateTrafficAccidents()
    val df1 = getNumberOfAccidentsPerSeason(loadTable("traffic_accidents"))
    val df2 = getNumberOfAccidentsPerSeasonPerType(loadTable("traffic_accidents"))
    val df3 = getMaxAccidentTypeByNeighborhood(loadTable("traffic_accidents"))
    val df4 = getAverageResponseTimePerNeighborhood(loadTable("service_requests"))
    val df5 = getAverageResponseTimePerSeason(loadTable("service_requests"))
    val df6 = getAverageResponseTimePerSeasonPerNeighborhood(loadTable("service_requests"))
    val df7 = getIncidentVolumeRankByDate(loadTable("service_requests"), loadTable("traffic_accidents"))
    val df8 = getTopIncidentVolumeDateByNeighborhood(loadTable("service_requests"), loadTable("traffic_accidents"))
    val df9 = getBottomIncidentVolumeDateByNeighborhood(loadTable("service_requests"), loadTable("traffic_accidents"))
    val nCount = getNeighborhoodCount()
    println("Number of Accidents Per Season: \n")
    df1.show(false)
    println("Number of Accidents Per Season Per Type: \n")
    df2.show(false)
    println("Neighborhoods with the largest number of accidents per accident type: \n")
    df3.show(false)
    println("Averagre response time per neighborhood: \n")
    df4.show(nCount, false)
    println("Average response time per season: \n")
    df5.show(nCount, false)
    println("Average response time per season per neighborhood: \n")
    df6.show(nCount * 4, false)
    println("Incident volume ranked by date: \n")
    df7.show(366, false)
    println("Top incident volume date by neighborhood: \n")
    df8.show(nCount, false)
    println("Lowest incident volume date by neighborhood: \n")
    df9.show(nCount, false)
  }

  //---------------------------------------------

  //This function creates the service_request table in Postgres if it does not already exist.
  //The function then performs an upsert to the service_request table of all the data stored
  //in the csv file stored server side at the following path:
  //C:\Users\Dakotah\dev\311_service_requests.csv
  //If the table required updating in the future, simply replacing this file and running
  //this function would update all the data in the table
  def updateServiceRequests(): DataFrame = {
    sql"""
         SET DATESTYLE = 'ISO,MDY';
         CREATE TABLE IF NOT EXISTS service_requests (
             case_summary VARCHAR(512),
             case_status VARCHAR(64),
             case_source VARCHAR(64),
             case_created_date TIMESTAMP,
             case_created_dttm TIMESTAMP,
             case_closed_date TIMESTAMP,
             case_closed_dttm TIMESTAMP,
             first_call_resolution VARCHAR(3),
             customer_zip_code VARCHAR(12),
             incident_address_1 VARCHAR(256),
             incident_address_2 VARCHAR(256),
             incident_intersection_1 VARCHAR(256),
             incident_intersection_2 VARCHAR(256),
             incident_zip_code VARCHAR(12),
             longitude DOUBLE PRECISION,
             latitude DOUBLE PRECISION,
             agency VARCHAR(64),
             division VARCHAR(128),
             major_area VARCHAR(128),
             type VARCHAR(64),
             topic VARCHAR(128),
             council_district VARCHAR(12),
             Police_district VARCHAR(12),
             neighborhood VARCHAR(128)
          );
             CREATE TEMP TABLE temp_sr AS SELECT * FROM service_requests LIMIT 0;
             COPY temp_sr (
               case_summary,
               case_status,
               case_source,
               case_created_date,
               case_created_dttm,
               case_closed_date,
               case_closed_dttm,
               first_call_Resolution,
               customer_zip_code,
               incident_address_1,
               incident_address_2,
               incident_intersection_1,
               incident_intersection_2,
               incident_zip_code,
               longitude,
               latitude,
               agency,
               division,
               major_area,
               type,
               topic,
               council_district,
               police_district,
               neighborhood)
             FROM 'C:\Users\Dakotah\dev\311_service_requests.csv' WITH (DELIMITER ',', FORMAT CSV, HEADER 1, FORCE_NULL(case_created_date, case_created_dttm, case_closed_date, case_closed_dttm, longitude, latitude, neighborhood));
             BEGIN;
             LOCK TABLE service_requests IN SHARE ROW EXCLUSIVE MODE;
             WITH upsert AS (UPDATE service_requests SET case_Summary = temp_sr.case_summary, case_status = temp_sr.case_status,case_created_date = temp_sr.case_created_date,
               case_created_dttm = temp_sr.case_created_dttm, case_closed_date = temp_sr.case_closed_date, case_closed_dttm = temp_sr.case_closed_dttm,
               first_call_resolution = temp_sr.first_call_resolution,customer_zip_code = temp_sr.customer_zip_code,incident_address_1 = temp_sr.incident_address_1,
               incident_address_2 = temp_sr.incident_address_2,incident_intersection_1 = temp_sr.incident_intersection_1, incident_intersection_2 = temp_sr.incident_intersection_2,
               incident_zip_code = temp_sr.incident_zip_code,longitude = temp_sr.longitude,latitude = temp_sr.latitude,agency = temp_sr.agency,division = temp_sr.division,
               major_area = temp_sr.major_area,type = temp_sr.type,topic = temp_sr.topic, council_district = temp_sr.council_district, police_district = temp_sr.police_district,
               neighborhood = temp_sr.neighborhood FROM temp_sr
             WHERE service_requests.case_summary = temp_sr.case_summary AND service_requests.case_created_dttm = temp_sr.case_created_dttm RETURNING *)
             INSERT INTO service_requests(case_summary,case_status,case_created_date,case_created_dttm,case_closed_date,case_closed_dttm,first_call_resolution,
                                          customer_zip_code,incident_address_1,incident_address_2,incident_intersection_1,incident_intersection_2,incident_zip_code,longitude,latitude,agency,
                                          division,major_area,type,topic,council_district,police_district,neighborhood)
               SELECT temp_sr.case_summary,temp_sr.case_status,temp_sr.case_created_date,temp_sr.case_created_dttm,temp_sr.case_closed_date,temp_sr.case_closed_dttm,
                 temp_sr.first_call_resolution,temp_sr.customer_zip_code,temp_sr.incident_address_1,temp_sr.incident_address_2,temp_sr.incident_intersection_1,
                 temp_sr.incident_intersection_2,temp_sr.incident_zip_code,temp_sr.longitude,temp_sr.latitude,temp_sr.agency,temp_sr.division,temp_sr.major_area,
                 temp_sr.type,temp_sr.topic,temp_sr.council_district,temp_sr.police_district,temp_sr.neighborhood FROM temp_sr
               WHERE NOT EXISTS(SELECT * FROM upsert);
             COMMIT;
             DROP TABLE temp_sr;
    """.execute().apply()
    loadTable("service_requests")
  }

  //This function creates the traffic_accidents table in Postgres if it does not already exist.
  //The function then performs an upsert to the traffic_accidents table of all the data stored
  //in the csv file stored server side at the following path:
  //C:\Users\Dakotah\dev\traffic_accidents.csv
  //If the table required updating in the future, simply replacing this file and running
  //this function would update all the data in the table
  def updateTrafficAccidents(): DataFrame = {
    sql"""
          SET DATESTYLE = 'ISO,MDY';
          CREATE TABLE IF NOT EXISTS traffic_accidents (
          incident_id DOUBLE PRECISION NOT NULL,
          offense_id DOUBLE PRECISION,
          offense_code INT,
          offense_code_extension INT,
          offense_type_id VARCHAR(64),
          offense_category_id VARCHAR(64),
          first_occurrence_date TIMESTAMP,
          last_occurrence_date TIMESTAMP,
          reported_date TIMESTAMP,
          incident_address VARCHAR(256),
          geo_x DOUBLE PRECISION,
          geo_y DOUBLE PRECISION,
          geo_lon DOUBLE PRECISION,
          geo_lat DOUBLE PRECISION,
          district_id INT,
          precinct_id INT,
          neighborhood_id VARCHAR(128),
          bicycle_ind INT,
          pedestrian_ind INT);
         CREATE TEMP TABLE temp_ta AS SELECT * FROM traffic_accidents LIMIT 0;
         COPY temp_ta (
         incident_id,
         offense_id,
         offense_code,
         offense_code_extension,
         offense_type_id,
         offense_category_id,
         first_occurrence_date,
         last_occurrence_date,
         reported_date,
         incident_address,
         geo_x,
         geo_y,
         geo_lon,
         geo_lat,
         district_id,
         precinct_id,
         neighborhood_id,
         bicycle_ind,
         pedestrian_ind)
         FROM 'C:\Users\Dakotah\dev\traffic_accidents.csv' WITH (DELIMITER ',', FORMAT CSV, HEADER 1, FORCE_NULL(first_occurrence_date, last_occurrence_date, reported_date,geo_lon,geo_lat));
         BEGIN;
         LOCK TABLE traffic_accidents IN SHARE ROW EXCLUSIVE MODE;
         WITH upsert AS (UPDATE traffic_accidents SET incident_id = temp_ta.incident_id, offense_id = temp_ta.offense_id, offense_code = temp_ta.offense_code, offense_code_extension = temp_ta.offense_code_extension,
         offense_type_id = temp_ta.offense_type_id, offense_category_id = temp_ta.offense_category_id, first_occurrence_date = temp_ta.first_occurrence_date,last_occurrence_date = temp_ta.last_occurrence_date,
         reported_date = temp_ta.reported_date, incident_address = temp_ta.incident_address, geo_x = temp_ta.geo_x, geo_y = temp_ta.geo_y, geo_lon = temp_ta.geo_lon, geo_lat = temp_ta.geo_lat, district_id = temp_ta.district_id,
         precinct_id = temp_ta.precinct_id, neighborhood_id = temp_ta.neighborhood_id, bicycle_ind = temp_ta.bicycle_ind, pedestrian_ind = temp_ta.pedestrian_ind FROM temp_ta
         WHERE traffic_accidents.incident_id = temp_ta.incident_id AND traffic_accidents.reported_date = temp_ta.reported_date AND traffic_accidents.offense_type_id = temp_ta.offense_type_id RETURNING *)
         INSERT INTO traffic_accidents(incident_id,offense_id,offense_code,offense_code_extension,offense_type_id,offense_category_id,first_occurrence_date,last_occurrence_date,reported_date,incident_address,geo_x,geo_y,geo_lon,
         geo_lat,district_id,precinct_id,neighborhood_id,bicycle_ind,pedestrian_ind)
         SELECT temp_ta.incident_id, temp_ta.offense_id, temp_ta.offense_code, temp_ta.offense_code_extension, temp_ta.offense_type_id, temp_ta.offense_category_id, temp_ta.first_occurrence_date, temp_ta.last_occurrence_date,
         temp_ta.reported_date, temp_ta.incident_address, temp_ta.geo_x, temp_ta.geo_y, temp_ta.geo_lon, temp_ta.geo_lat, temp_ta.district_id, temp_ta.precinct_id, temp_ta.neighborhood_id, temp_ta.bicycle_ind, temp_ta.pedestrian_ind
         FROM temp_ta
         WHERE NOT EXISTS(SELECT * FROM upsert);
         COMMIT;
         DROP TABLE temp_ta;
       """.execute().apply()
    loadTable("traffic_accidents")
  }

  //This function loads the table specified by the tableName parameter
  //into the spark session
  def loadTable(tableName: String): DataFrame = {
    val table: DataFrame = spark.read.format("jdbc")
      .option("url", DB_PATH)
      .option("user", USERNAME)
      .option("password", PASSWORD)
      .option("dbtable", tableName)
      .load()
    table
  }

  //These functions each create a dataframe with the data specified by the name of the function
  //-----------------------------------------------------------------------------------------------
  def getNumberOfAccidentsPerSeason(traffic_accidents: DataFrame): DataFrame = {
    traffic_accidents.select($"offense_type_id",
      when(date_format($"first_occurrence_date", "MM-dd").between("03-20", "06-20"), "spring")
        .when(date_format($"first_occurrence_date", "MM-dd").between("06-21", "09-21"), "summer")
        .when(date_format($"first_occurrence_date", "MM-dd").between("09-22", "12-20"), "fall").otherwise("winter").alias("season"))
      .groupBy("season").agg(count($"offense_type_id").alias("accidents_per_season"))
  }

  def getNumberOfAccidentsPerSeasonPerType(traffic_accidents: DataFrame): DataFrame = {
    traffic_accidents.select($"offense_type_id", $"offense_type_id",
      when(date_format($"first_occurrence_date", "MM-dd").between("03-20", "06-20"), "spring")
        .when(date_format($"first_occurrence_date", "MM-dd").between("06-21", "09-21"), "summer")
        .when(date_format($"first_occurrence_date", "MM-dd").between("09-22", "12-20"), "fall").otherwise("winter").alias("season"))
      .groupBy("season", "offense_type_id").agg(count($"offense_type_id").alias("accidents_per_season")).orderBy("offense_type_id", "season")
  }

  def getMaxAccidentTypeByNeighborhood(traffic_accidents: DataFrame): DataFrame = {
    val temp_df1 = traffic_accidents.select($"neighborhood_id", $"offense_type_id")
      .groupBy($"neighborhood_id", $"offense_type_id").agg(count($"*").alias("crime_count"))
    val temp_df2 = temp_df1.select($"crime_count", $"neighborhood_id", $"offense_type_id",
      row_number().over(Window.partitionBy("offense_type_id").orderBy(desc("crime_count"))).alias("row_count"))
    temp_df2.select($"crime_count", $"neighborhood_id", $"offense_type_id").filter($"row_count" === 1)
  }

  def getAverageResponseTimePerNeighborhood(service_requests: DataFrame): DataFrame = {
    val tempdf1 = service_requests.select($"case_closed_dttm", $"case_created_dttm", $"neighborhood", datediff($"case_closed_dttm", $"case_created_dttm").alias("date_diff")).filter("date_diff IS NOT NULL")
      .groupBy($"neighborhood").agg(sum($"date_diff").alias("date_sum"), count($"date_diff").alias("date_count"))
    tempdf1.select(($"date_sum" / $"date_count").alias("average_response_time"), $"neighborhood").orderBy($"neighborhood")
  }

  def getAverageResponseTimePerSeason(service_requests: DataFrame): DataFrame = {
    val tempdf1 = service_requests.select($"case_closed_dttm", $"case_created_dttm", datediff($"case_closed_dttm", $"case_created_dttm").alias("date_diff"),
      when(date_format($"case_created_dttm", "MM-dd").between("03-20", "06-20"), "spring")
        .when(date_format($"case_created_dttm", "MM-dd").between("06-21", "09-21"), "summer")
        .when(date_format($"case_created_dttm", "MM-dd").between("09-22", "12-20"), "fall").otherwise("winter").alias("season")).filter("date_diff IS NOT NULL")
      .groupBy($"season").agg(sum($"date_diff").alias("date_sum"), count($"date_diff").alias("date_count"))
    tempdf1.select(($"date_sum" / $"date_count").alias("average_response_time"), $"season").orderBy($"season")
  }

  def getAverageResponseTimePerSeasonPerNeighborhood(service_requests: DataFrame): DataFrame = {
    val tempdf1 = service_requests.select($"case_closed_dttm", $"case_created_dttm", $"case_created_date", $"neighborhood",
      datediff($"case_closed_dttm", $"case_created_dttm").alias("date_diff"),
      when(date_format($"case_created_date", "MM-dd").between("03-20", "06-20"), "spring")
        .when(date_format($"case_created_date", "MM-dd").between("06-21", "09-21"), "summer")
        .when(date_format($"case_created_date", "MM-dd").between("09-22", "12-20"), "fall").otherwise("winter").alias("season")).filter("date_diff IS NOT NULL")
      .groupBy($"season", $"neighborhood").agg(sum($"date_diff").alias("date_sum"), count($"date_diff").alias("date_count"))
    tempdf1.select(($"date_sum" / $"date_count").alias("average_response_time"), $"season", $"neighborhood").orderBy($"neighborhood", $"season")
  }

  def getIncidentVolumeRankByDate(service_requests: DataFrame, traffic_accidents: DataFrame): DataFrame = {
    //Build traffic_accidents side of the join
    val trafficGroupTemp = traffic_accidents.select(date_format($"reported_date", "MM-dd").alias("traffic_date"), $"reported_date")
    val trafficCount = trafficGroupTemp.select($"traffic_date", $"reported_date").groupBy("traffic_date").agg(count($"traffic_date").alias("countt"))
    val trafficGroup = trafficCount.select($"traffic_date", $"countt", row_number().over(Window.orderBy(desc("countt"))).alias("rank"))

    //Build service_requests side of the join
    val serviceGroupTemp = service_requests.select(date_format($"case_created_date", "MM-dd").alias("service_date"), $"case_created_date")
    val serviceCount = serviceGroupTemp.select($"service_date", $"case_created_date").groupBy("service_date").agg(count($"service_date").alias("counts"))
    val serviceGroup = serviceCount.select($"service_date", $"counts", row_number().over(Window.orderBy(desc("counts"))).alias("rank"))

    //Join groups and select appropriate columns
    trafficGroup.join(serviceGroup, "rank").select(
      $"rank",
      $"traffic_date".alias("traffic_accident_date"),
      $"countt".alias("num_traffic_accidents"),
      $"service_date".alias("service_request_date"),
      $"counts".alias("num_service_requests"))
  }

  def getTopIncidentVolumeDateByNeighborhood(service_requests: DataFrame, traffic_accidents: DataFrame): DataFrame = {
    //Build traffic_accidents side of the join
    val trafficGroupTemp = traffic_accidents.select($"neighborhood_id", date_format($"reported_date", "MM-dd").alias("traffic_date"))
    val trafficCount = trafficGroupTemp.select($"neighborhood_id", $"traffic_date").groupBy("traffic_date", "neighborhood_id").agg(count($"traffic_date").alias("countt"))
    val trafficRow = trafficCount.select($"countt", $"neighborhood_id", $"traffic_date", row_number().over(Window.partitionBy($"neighborhood_id").orderBy(desc("countt"))).alias("row_countt"))
    val trafficGroup = trafficRow.select($"countt", $"neighborhood_id", $"traffic_date", lower(regexp_replace($"neighborhood_id", " |-", "")).alias("trimn")).where($"row_countt" === 1)

    //Build service_requests side of the join
    val serviceGroupTemp = service_requests.select($"neighborhood", date_format($"case_created_date", "MM-dd").alias("service_date"))
    val serviceCount = serviceGroupTemp.select($"neighborhood", $"service_date").groupBy("service_date", "neighborhood").agg(count($"service_date").alias("counts"))
    val serviceRow = serviceCount.select($"counts", $"neighborhood", $"service_date", row_number().over(Window.partitionBy($"neighborhood").orderBy(desc("counts"))).alias("row_counts"))
    val serviceGroup = serviceRow.select($"counts", $"neighborhood", $"service_date", lower(regexp_replace($"neighborhood", " |-", "")).alias("trimn")).where($"row_counts" === 1)

    //Join groups and select appropriate columns
    trafficGroup.join(serviceGroup, "trimn").select(
      $"countt".alias("num_traffic_accidents"),
      $"traffic_date".alias("traffic_accident_date"),
      $"counts".alias("num_service_requests"),
      $"service_date".alias("service_request_date"), $"neighborhood").orderBy($"neighborhood")
  }

  def getBottomIncidentVolumeDateByNeighborhood(service_requests: DataFrame, traffic_accidents: DataFrame): DataFrame = {

    //Build traffic_accidents side of the join
    val trafficGroupTemp = traffic_accidents.select($"neighborhood_id", date_format($"reported_date", "MM-dd").alias("traffic_date"))
    val trafficCount = trafficGroupTemp.select($"neighborhood_id", $"traffic_date").groupBy("traffic_date", "neighborhood_id").agg(count($"traffic_date").alias("countt"))
    val trafficRow = trafficCount.select($"countt", $"neighborhood_id", $"traffic_date", row_number().over(Window.partitionBy($"neighborhood_id").orderBy(asc("countt"))).alias("row_countt"))
    val trafficGroup = trafficRow.select($"countt", $"neighborhood_id", $"traffic_date", lower(regexp_replace($"neighborhood_id", " |-", "")).alias("trimn")).where($"row_countt" === 1)

    //Build service_requests side of the join
    val serviceGroupTemp = service_requests.select($"neighborhood", date_format($"case_created_date", "MM-dd").alias("service_date"))
    val serviceCount = serviceGroupTemp.select($"neighborhood", $"service_date").groupBy("service_date", "neighborhood").agg(count($"service_date").alias("counts"))
    val serviceRow = serviceCount.select($"counts", $"neighborhood", $"service_date", row_number().over(Window.partitionBy($"neighborhood").orderBy(asc("counts"))).alias("row_counts"))
    val serviceGroup = serviceRow.select($"counts", $"neighborhood", $"service_date", lower(regexp_replace($"neighborhood", " |-", "")).alias("trimn")).where($"row_counts" === 1)

    //Join groups and select appropriate columns
    trafficGroup.join(serviceGroup, "trimn").select(
      $"countt".alias("num_traffic_accidents"),
      $"traffic_date".alias("traffic_accident_date"),
      $"counts".alias("num_service_requests"),
      $"service_date".alias("service_request_date"), $"neighborhood").orderBy($"neighborhood")
  }

  //-----------------------------------------------------------------------------------------------

  //This function returns the number of neighborhoods in the service_requests table
  def getNeighborhoodCount(): Int = {
    val service_requests = loadTable("service_requests")
    val neighborhoodCountDf = service_requests.agg(countDistinct($"neighborhood"))
    neighborhoodCountDf.head().getLong(0).toInt + 1
  }
}
