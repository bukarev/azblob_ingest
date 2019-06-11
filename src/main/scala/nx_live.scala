import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.typedLit
import scala.util.matching.Regex
import scala.util.{Try,Success,Failure}
import org.apache.hadoop.fs.{FileSystem,Path,FileUtil,FileStatus}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object nxLive {

def main(args: Array[String]) {

    val sysLayout = { if (args.size > 0) args(0) else "dev" }

    val inPath = "/clickstream/"+sysLayout+"/stage/live/"
    val outPath = "/clickstream/" + sysLayout + "/report/clickstream/"
    
    val outTable = { if (args.size > 1) args(1) else sysLayout+".clickstream" }
    
//======== Define helper UDFs and regexes
                            val getDate = udf((x: String) => {
                                val pn = "^<\\d*>(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})* ([^ ]*) netxpress(.*?)\\[\\d+\\]: (\\{.*\\})*".r
                                x match {
                                    case pn(d,_,_,_) => d
                                    case x => ""
                                }
                            })
                            
                            val getYear = udf((d: String) => {
                                if (d != "") { LocalDateTime.parse(d,DateTimeFormatter.ISO_OFFSET_DATE_TIME).getYear() } else {0}
                            })
                            
                            val getMonth = udf((d: String) => {
                                if (d != "") { LocalDateTime.parse(d,DateTimeFormatter.ISO_OFFSET_DATE_TIME).getMonthValue()  } else {0}
                            })
                            
                            val getDayOfMonth = udf((d: String) => {
                                if (d != "") { LocalDateTime.parse(d,DateTimeFormatter.ISO_OFFSET_DATE_TIME).getDayOfMonth() } else {0}
                            })
                            
                            val getDayOfYear = udf((d: String) => {
                                if (d != "") { LocalDateTime.parse(d,DateTimeFormatter.ISO_OFFSET_DATE_TIME).getDayOfYear() } else {0}
                            })
                            
                            val getHour = udf((d: String) => {
                                if (d != "") { LocalDateTime.parse(d,DateTimeFormatter.ISO_OFFSET_DATE_TIME).getHour() } else {99}
                            })
                            
                            val shrinkArray = udf((x: Array[String]) => {
                                x.mkString(",")
                            })
     
                            val getJSON = udf((x: String) => {
                                val pn = "^<\\d*>(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})* ([^ ]*) netxpress(.*?)\\[\\d+\\]: \\{(.*)\\}".r
                                x match {
                                    case pn(_,_,_,j) => j
                                    case x => ""
                                }
                            })
     
                            val parseId = udf((x: Long) => {
                               "\"spark_id\":"+x.toString
                            })
                            
                            val partitionColumns = Seq("year", "month", "day_of_month", "sourcefilename")
//======== End of helper UDFs and regexes

val sparkSess = SparkSession.builder()
                 .appName("Migrate Live logs to Azure")
                 .enableHiveSupport()
                 .getOrCreate()

import sparkSess.implicits._
val sc = sparkSess.sparkContext

// Get the list of already loaded files
val loadedList = sc.broadcast(sparkSess.sql("select filename, sum(status) as status from " + sysLayout + ".load_log group by filename").filter(col("status").notEqual(0)).collect().map(x => x(0)))

// Generate the schema based on the string of schema  
val logSchema = StructType(Array(
                            StructField("load_date", StringType),
                            StructField("nx_date", StringType),
                            StructField("filename", StringType),
                            StructField("status", IntegerType)
                            ))
                            
// Get the list of directories, and per each (i.e. calday) read all the files, then process them together

    var fs = FileSystem.get(sc.hadoopConfiguration)
    // From the directories list, only pick those up to yesteday's date
    val dirList = FileUtil.stat2Paths(fs.listStatus( new Path(inPath))) //.filter(d => LocalDate.parse(d.getName,DateTimeFormatter.BASIC_ISO_DATE).isBefore(LocalDate.now))

    dirList.map( d => {
        val dName = d.getName()

        val fileList = FileUtil.stat2Paths(fs.listStatus( new Path(inPath+dName))).map(f => f.getName).filter(f => f matches "nxmetrics.*").filter(f => !loadedList.value.contains(inPath+dName+"/"+f))
        
        fileList.map(fileName => {
        
            println("Migrating: " + inPath + dName + "/" + fileName);
                
            val tRead = Try(sparkSess.read.json(inPath+dName+"/" + fileName).select(col("raw")))//.repartition(5)
            tRead match {
            
                case Success(nx_live) => {
                    println("Lines here: " + nx_live.count())
                    val t = Try(nx_live.first)

                    t match {            
                        case Success(t) => {

                            val dfLive = nx_live.withColumn("spark_id",monotonicallyIncreasingId).repartition(10)
                
                            val dfWrap = dfLive.withColumn("logdate",getDate(col("raw")))
                                               .select(col("spark_id"),col("logdate"))
                                               .withColumn("year",getYear(col("logdate")))
                                               .withColumn("month",getMonth(col("logdate")))
                                               .withColumn("day_of_month",getDayOfMonth(col("logdate")))
                                               .withColumn("day_of_year",getDayOfYear(col("logdate")))
                                               .withColumn("hour",getHour(col("logdate")))
                                               .withColumn("sourcefilename",typedLit(fileName))
                
                            val dfJSON = dfLive.withColumn("nx_json",getJSON(col("raw"))).filter(col("nx_json").notEqual("")).select(col("spark_id"),col("nx_json")).withColumn("spid",parseId(col("spark_id"))).select(col("nx_json"),col("spid")).map(row => row.mkString(",")).map(json => "{" + json + "}")
                
                            val n = Try(dfWrap.join(sparkSess.read.json(dfJSON), "spark_id").drop(col("spark_id")))
                
                            n match {
                                case Success(newDF) => {
                                
                                    val fieldLst = newDF.schema.fields
                                    val renDF = fieldLst.foldLeft(newDF){(df,f) => { 
                                        if (f.name.matches("^feature_(.*)[\\. ,;{}()]+(.*)") ||  // like "feature_balbla ablabl ablabla"
                                            f.name.matches("(^-*\\d*)(\\.\\d+)?$")           ||  // all numeric field with optional sign
                                            f.name == "feature_match_mode%"                      // one-time bug
                                            ) { 
                                            df.drop(f.name)
                                        } else {    
                                            if (f.dataType.typeName == "array") {
                                                df.withColumn(f.name.replace('-','_'),shrinkArray(col(f.name))).drop(col(f.name))
                                            }
                                            else df.withColumnRenamed( f.name, f.name.replace("-","_")) }
                                        }    
                                    }
                            
                            // Alter the output hive table if needed
                                    val oldList = sparkSess.catalog.listColumns(outTable).select(col("name"),col("dataType")).collect.map(x => x match {
                                        case Row(f,t) => (f,t)
                                      }).toMap

                                    val newList = renDF.schema.fields.map( x => (x.name,x.dataType.typeName)).toMap;
                           
                           // Check if any additional columns have to be added to the Hive table
                                    val addCol = newList.filterKeys(k => !oldList.keySet.exists(_ == k)).foldLeft(""){(r,f) => {if(r=="") "" else r+", "} + f._1 + " " + { if (f._2 == "long" ) "bigint" else f._2 } }
                                    
                                    var hiveSucc = 0; // flag for Hive alter table success
                                    
                                    if (addCol != "") { 
                                        val h = Try(sparkSess.sql("alter table " + outTable + " add columns(" + addCol + ")"));
                                        
                                        h match {
                                             case Success(hs) => {
                                                 hiveSucc = 1;
                                             }
                                             case Failure(hf) => {
                                                 println("Hive error, file not migrated: " + inPath + dName + "/" + fileName + ": " + hf.toString)
                                                 hiveSucc = 1;
                                             } // end of failure handling
                                        } // end of h match
                                    }
                            
                            // Check if any additional columns have to be added to the current data
                                    //val extraCol = oldList.filterKeys(k => !newList.keySet.exists(_ == k))
                                    //val writeDF = extraCol.foldLeft(renDF){(df,f) => df.withColumn(f._1.toString, typedLit(null))    }
                                    val writeDF = renDF
                                            
                            // Write file and update log
                                    if (hiveSucc == 0) {
                                        // Update log with the list of migrated files
                                        //println("Writing to: " + outPath + dName.toString + "/" + fileName + ".json")
                                        //renDF.write.format("parquet").save(outPath + dName.toString + "/" + fileName + ".json")
                                        
                                        
                                        // Put partition columns at the end of the dataset
                                        val columns = writeDF.schema.collect{ case s if !partitionColumns.contains(s.name) => s.name} ++ partitionColumns
                                        
                                        val (wYear,wMonth,wDay,wFile) = writeDF.select("year","month","day_of_month","sourcefilename").as[(Int,Int,Int,String)].take(1)(0)
                                        val writePath = "/clickstream/prod/report/clickstream/" +
                                                        "year="+wYear+"/"+
                                                        "month="+wMonth+"/"+
                                                        "day_of_month="+wDay+"/"+
                                                        wFile
                                        
                                        val w = Try({ 
                                                        writeDF.select(columns.head, columns.tail: _*)
                                                         //.repartition($"year",$"month",$"day_of_month",$"hour",$"accesslevel",$"channel",$"event_name",$"gcode",$"group_id",$"sourcefilename")
                                                         .repartition(1, $"year",$"month", $"day_of_month", $"sourcefilename")
                                                         .write
                                                         //.partitionBy("year","month","day_of_month","sourcefilename")
                                                         .mode(SaveMode.Overwrite).parquet(writePath)
                                                         //.write.format("hive").mode(SaveMode.Append).insertInto(outTable)
                                                         
                                                        sparkSess.sql(s"alter table $outTable drop if exists partition (year=$wYear,month=$wMonth,day_of_month=$wDay,sourcefilename='$wFile')")
                                                        sparkSess.sql(s"alter table $outTable add if not exists partition (year=$wYear,month=$wMonth,day_of_month=$wDay,sourcefilename='$wFile') location '$writePath'")
                                                }) // end of Try write
                                        w match {
                                                     case Success(s) => {        
                                                         val logUpdate = sparkSess.createDataFrame(sc.parallelize(Seq(fileName)).map(f => Row(java.time.LocalDate.now.toString.replace("-",""), dName, inPath+dName+"/"+f, 1)),logSchema)
                                        
                                                         logUpdate.write.format("hive").mode(SaveMode.Append).saveAsTable(sysLayout+".load_log")
                                                         println("Migrated: " + fileName + " into " + outTable)
                                                     } // end of Success
                                                     case Failure(e) => {
                                                         println("Can't write file: " + e.toString)
                                                     }
                                                } // end of match w 
                                    }    
            
                                }
                
                                case Failure(newDF) => { println("Skip empty folder: " + dName.toString)}
                            }
                        }
              
                        case Failure(t) => { println("Skip empty file: " + dName.toString)  } // do nothing
                    } // end of if-check for dataframe size    
                }
                case Failure(f) => { println("Nothing in directory: " + dName.toString)  } // do nothing
                
            } // end of check for file read success
            
        }) // of looping through file list
        
    }) // end of foreach dir    

}
}

