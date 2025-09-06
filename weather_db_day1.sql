-- MySQL dump 10.13  Distrib 8.4.6, for Win64 (x86_64)
--
-- Host: localhost    Database: weather_db
-- ------------------------------------------------------
-- Server version	8.4.6

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `weather_db`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `weather_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE `weather_db`;

--
-- Table structure for table `city`
--

DROP TABLE IF EXISTS `city`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `city` (
  `city_id` smallint NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `province` varchar(30) NOT NULL,
  `lat` decimal(8,5) DEFAULT NULL,
  `lng` decimal(8,5) DEFAULT NULL,
  PRIMARY KEY (`city_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `city`
--

LOCK TABLES `city` WRITE;
/*!40000 ALTER TABLE `city` DISABLE KEYS */;
INSERT INTO `city` VALUES (1,'北京','北京市',39.90420,116.40740),(2,'上海','上海市',31.23040,121.47370),(3,'广州','广东省',23.12910,113.26440);
/*!40000 ALTER TABLE `city` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `weather_daily`
--

DROP TABLE IF EXISTS `weather_daily`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `weather_daily` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `city_id` smallint NOT NULL,
  `date` date NOT NULL,
  `temp_max_c` tinyint DEFAULT NULL,
  `temp_min_c` tinyint DEFAULT NULL,
  `temp_avg_c` tinyint DEFAULT NULL,
  `rh_avg` tinyint DEFAULT NULL,
  `wind_kmh` tinyint DEFAULT NULL,
  `pm25` smallint DEFAULT NULL,
  `weather_code` tinyint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_weather_city_date` (`city_id`,`date`),
  CONSTRAINT `fk_weather_city` FOREIGN KEY (`city_id`) REFERENCES `city` (`city_id`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `weather_daily`
--

LOCK TABLES `weather_daily` WRITE;
/*!40000 ALTER TABLE `weather_daily` DISABLE KEYS */;
INSERT INTO `weather_daily` VALUES (1,1,'2024-09-01',32,22,27,65,12,45,1),(2,1,'2024-09-02',30,21,25,70,15,55,2),(3,1,'2024-09-03',28,20,24,80,18,80,3),(4,1,'2024-09-04',29,21,25,75,14,60,2),(5,1,'2024-09-05',31,22,26,68,10,40,1),(6,1,'2024-09-06',33,23,28,60,11,35,1),(7,1,'2024-09-07',34,24,29,55,9,30,1),(8,2,'2024-09-01',30,24,27,70,14,40,1),(9,2,'2024-09-02',29,23,26,75,16,50,2),(10,2,'2024-09-03',27,22,24,85,20,70,3),(11,2,'2024-09-04',28,23,25,80,15,55,2),(12,2,'2024-09-05',30,24,27,72,12,35,1),(13,2,'2024-09-06',31,25,28,65,10,30,1),(14,2,'2024-09-07',32,26,29,60,8,25,1),(15,3,'2024-09-01',33,26,29,80,10,35,1),(16,3,'2024-09-02',32,25,28,82,12,45,2),(17,3,'2024-09-03',31,24,27,85,14,60,3),(18,3,'2024-09-04',32,25,28,83,13,50,2),(19,3,'2024-09-05',33,26,29,78,11,30,1),(20,3,'2024-09-06',34,27,30,75,9,25,1),(21,3,'2024-09-07',35,28,31,70,8,20,1);
/*!40000 ALTER TABLE `weather_daily` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-09-06 22:44:52
