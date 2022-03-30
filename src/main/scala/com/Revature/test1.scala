package com.Revature
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn

object test1 {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\Users\\tahmi\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
//    spark.sql("create database games")
    spark.sql("use games")
//    spark.sql("show tables").show()
//    Loading Data into pc_games table
//      spark.sql("CREATE TABLE IF NOT EXISTS pc_games(id INT, game_name String, genre String) row format delimited fields terminated by ','");
//      spark.sql("LOAD DATA LOCAL INPATH 'pc_games.txt' INTO TABLE pc_games")
//    Loading data into mobile_games table
//      spark.sql("CREATE TABLE IF NOT EXISTS mobile_games(id INT, game_name String, genre String) row format delimited fields terminated by ','");
//      spark.sql("LOAD DATA LOCAL INPATH 'mobile_games.txt' INTO Table mobile_games")
//    Loading data into pc_games_sold
//        spark.sql("CREATE TABLE IF NOT EXISTS pc_games_sold(id Int, game_name String, copies_sold Int) row format delimited fields terminated by ','");
//        spark.sql("LOAD DATA LOCAL INPATH 'pc_games_sold.txt' INTO Table pc_games_sold")
//    Loading data into mobile games sold
//    spark.sql("CREATE Table IF NOT EXISTS mobile_games_sold(id Int, game_name String, copies_sold Int) row format delimited fields terminated by ','");
//    spark.sql("LOAD DATA LOCAL INPATH 'mobile_games_sold.txt' INTO Table mobile_games_sold")


    def showPCgames(): Unit =
    {
      spark.sql("SELECT * FROM pc_games").show
    }
    def showMobileGames(): Unit =
    {
      spark.sql("SELECT * FROM mobile_games").show
    }
  def showSalesPCgames(): Unit =
    {
      spark.sql("SELECT * FROM pc_games_sold").show
    }
  def showSalesMobilegames()
    {
      spark.sql("SELECT * FROM mobile_games_sold").show
    }

      def question1(): Unit =
    {
      spark.sql("""SELECT DISTINCT pc_games.game_name FROM pc_games WHERE genre = "Survival/Horror" """).show()
    }
    def question2(): Unit =
    {
      spark.sql("SELECT sum(mobile_games_sold.copies_sold) FROM mobile_games_sold").show
    }
    def question3(): Unit =
    {
      spark.sql("SELECT sum(pc_games_sold.copies_sold) FROM pc_games_sold").show
    }
    def question4(): Unit =
    {
      spark.sql("""SELECT DISTINCT mobile_games.game_name FROM mobile_games WHERE genre = "Strategy" """).show()
    }
    def question5()
    {
      spark.sql("""SELECT DISTINCT pc_games.game_name FROM pc_games WHERE genre = "Action/Adventure" """).show()
    }
    def question6(): Unit =
    {
      spark.sql("""SELECT DISTINCT mobile_games.game_name FROM mobile_games WHERE genre = "Puzzle" """).show()
    }
    def insert(): Unit = {
      var done = false
      while (!done) {
        val table_name = readLine("Select in which table you want to insert data: \n1. pc_games, 2.mobile_games, 3.pc_games_sold, 4.mobile_games_sold: ")
        if (table_name == "1") {
          val id = readLine("Type Game ID: ")
          val game_name = readLine("Type pc game name: ")
          val genre = readLine("Type game genre: ")
          spark.sql(s"Insert into table pc_games values ($id,'$game_name','$genre')")
        } else if (table_name == "2") {
          val id = readLine("Type Game ID: ")
          val game_name = readLine("Type mobile game name: ")
          val genre = readLine("Type game genre: ")
          spark.sql(s"Insert into table mobile_games values ($id,'$game_name','$genre')")
        }
        else if (table_name == "3") {
          val id = readLine("Type Game ID: ")
          val game_name = readLine("Type pc game name: ")
          val sold = readLine("Type number of copies sold: ")
          spark.sql(s"Insert into table pc_games_sold values ($id,'$game_name',$sold)")
        }
        else if (table_name == "4") {
          val id = readLine("Type Game ID: ")
          val game_name = readLine("Type pc game name: ")
          val sold = readLine("Type number of copies sold: ")
          spark.sql(s"Insert into table mobile_games_sold values ($id,'$game_name',$sold)")
        }
        else {
          println("Invalid Input")
        }
        val input5 = readLine("Enter Y if done, n to try again: ")
        if(input5 == "y"||input5 == "Y"){done = true}
      }
    }
    def delete(): Unit = {
      var done = false
      while (!done) {
        val table_name = readLine("Select in which table you want to delete data from: \n1. pc_games, 2.mobile_games, 3.pc_games_sold, 4.mobile_games_sold: ")
        if (table_name == "1") {
          val id = readLine("Type Game ID: ")
          spark.sql(s"insert overwrite table pc_games select * from pc_games where id != $id")
        }
        else if (table_name == "2") {
          val id = readLine("Type Game ID: ")
          spark.sql(s"insert overwrite table mobile_games select * from mobile_games where id != $id")
        }
        else if (table_name == "3") {
          val id = readLine("Type Game ID: ")
          spark.sql(s"insert overwrite table pc_games_sold select * from pc_games_sold where id != $id")
        }
        else if (table_name == "4") {
          val id = readLine("Type Game ID: ")
          spark.sql(s"insert overwrite table mobile_games_sold select * from mobile_games_sold where id != $id")
        }
        else {
          println("Invalid Input")
        }
        val input5 = readLine("Enter Y if done, n to try again: ")
        if(input5 == "y"||input5 == "Y"){done = true}
      }
    }
    val menu1 = readLine("Do you want to view the tables?(y/n): ")
    if(menu1=="Y"||menu1=="y")
      {
        var done = false;
        while(!done)
        {
          println("SELECT  TO VIEW FROM THE TABLES BELOW:")
          println("1. PC GAMES")
          println("2. MOBILE GAMES")
          println("3. PC GAMES SOLD")
          println("4. MOBILE GAMES SOLD")
          val input1 = readLine("Enter from 1 to 4: ")
          if(input1 == "1"){showPCgames()}
          else if(input1 == "2") {showMobileGames()}
          else if(input1 =="3"){showSalesPCgames()}
          else if (input1=="4"){showSalesMobilegames()}
          else {println("INVALID INPUT")}

          val input2 = readLine("Enter Y if done, n to view another table: ")
          if(input2 == "y"||input2=="Y"){done = true}
        }
      } else
    {
      val menu2 = readLine("Do you want to view some queries?(y/n): ")
      if(menu2=="y"||menu2=="Y")
        {
          var success = false;
          while(!success)
          {
            println("SELECT  ONE OF THE QUERIES FROM BELOW:")
            println("1. WHAT ARE THE SURVIVAL/HORROR PC GAMES?")
            println("2. WHAT IS THE TOTAL NUMBER OF MOBILE GAMES SOLD?")
            println("3. WHAT IS THE TOTAL NUMBER OF PC GAMES SOLD?")
            println("4. WHAT ARE THE MOBILE STRATEGY GAMES?")
            println("5. WHAT ARE THE ACTION/ADVENTURE PC GAMES?")
            println("6. WHAT ARE THE MOBILE PUZZLE GAMES?")

            val input3 = readLine("Enter from 1 to 6: ")
            if(input3 == "1") {question1()}
            else if (input3 =="2"){question2()}
            else if(input3 == "3") {question3()}
            else if(input3 == "4") {question4()}
            else if(input3 == "5") {question5()}
            else if(input3 == "6") {question6()}
            else {println("Invalid input")}

            val input4 = readLine("Enter Y if done, n to answer another query: ")
            if(input4 == "y"||input4=="Y"){success = true}
          }
        } else
        {
          var done0 = true
          while(done0)
          {
            var input6 = readLine("PRESS 1 TO INSERT DATA, 2 TO DELETE: ")
            if(input6=="1") {insert()}
            else if(input6=="2") {delete()}
            else {println("Invalid Input")}

            val input7 = readLine("Enter Y if done, n to try again: ")
            if(input7 == "y"||input7 == "Y"){done0 = true}
          }
        }
    }

  }
}
