import java.util.Properties
import org.apache.kafka.clients.producer._
import org.json.JSONObject

import scala.util.Random


class Producer(topicName:String, n:Int, timePeriod:Int=100) {

  private val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val producer = new KafkaProducer[String, String](props)
  private val jsonObjectData = """{
        "data-0": {
            "id": 1,
            "firstName": "Maria",
            "lastName": "Anders",
            "createdTimestamp": "2022-12-21T19:18:57.605050600"
        },
        "data-1": {
            "id": 2,
            "firstName": "Ana",
            "lastName": "Trujillo",
            "createdTimestamp": "2022-12-21T19:18:57.606048200"
        },
        "data-2": {
            "id": 3,
            "firstName": "Antonio",
            "lastName": "Moreno",
            "createdTimestamp": "2022-12-21T19:18:57.606048200"
        },
        "data-3": {
            "id": 4,
            "firstName": "Thomas",
            "lastName": "Hardy",
            "createdTimestamp": "2022-12-21T19:18:57.606048200"
        },
        "data-4": {
            "id": 5,
            "firstName": "Christina",
            "lastName": "Berglund",
            "createdTimestamp": "2022-12-21T19:18:57.607055300"
        },
        "data-5": {
            "id": 6,
            "firstName": "Hanna",
            "lastName": "Moos",
            "createdTimestamp": "2022-12-21T19:18:57.608055500"
        },
        "data-6": {
            "id": 7,
            "firstName": "Frédérique",
            "lastName": "Citeaux",
            "createdTimestamp": "2022-12-21T19:18:57.608055500"
        },
        "data-7": {
            "id": 8,
            "firstName": "Martín",
            "lastName": "Sommer",
            "createdTimestamp": "2022-12-21T19:18:57.608055500"
        },
        "data-8": {
            "id": 9,
            "firstName": "Laurence",
            "lastName": "Lebihan",
            "createdTimestamp": "2022-12-21T19:18:57.609057500"
        },
        "data-9": {
            "id": 10,
            "firstName": "Elizabeth",
            "lastName": "Lincoln",
            "createdTimestamp": "2022-12-21T19:18:57.609057500"
        },
        "data-10": {
            "id": 11,
            "firstName": "Victoria",
            "lastName": "Ashworth",
            "createdTimestamp": "2022-12-21T19:18:57.609057500"
        },
        "data-11": {
            "id": 12,
            "firstName": "Patricio",
            "lastName": "Simpson",
            "createdTimestamp": "2022-12-21T19:18:57.610042500"
        },
        "data-12": {
            "id": 13,
            "firstName": "Francisco",
            "lastName": "Chang",
            "createdTimestamp": "2022-12-21T19:18:57.610042500"
        },
        "data-13": {
            "id": 14,
            "firstName": "Yang",
            "lastName": "Wang",
            "createdTimestamp": "2022-12-21T19:18:57.611043200"
        },
        "data-14": {
            "id": 15,
            "firstName": "Pedro",
            "lastName": "Afonso",
            "createdTimestamp": "2022-12-21T19:18:57.612051"
        },
        "data-15": {
            "id": 16,
            "firstName": "Elizabeth",
            "lastName": "Brown",
            "createdTimestamp": "2022-12-21T19:18:57.612051"
        },
        "data-16": {
            "id": 17,
            "firstName": "Sven",
            "lastName": "Ottlieb",
            "createdTimestamp": "2022-12-21T19:18:57.612051"
        },
        "data-17": {
            "id": 18,
            "firstName": "Janine",
            "lastName": "Labrune",
            "createdTimestamp": "2022-12-21T19:18:57.613051400"
        },
        "data-18": {
            "id": 19,
            "firstName": "Ann",
            "lastName": "Devon",
            "createdTimestamp": "2022-12-21T19:18:57.613051400"
        },
        "data-19": {
            "id": 20,
            "firstName": "Roland",
            "lastName": "Mendel",
            "createdTimestamp": "2022-12-21T19:18:57.614052900"
        },
        "data-20": {
            "id": 21,
            "firstName": "Aria",
            "lastName": "Cruz",
            "createdTimestamp": "2022-12-21T19:18:57.614052900"
        },
        "data-21": {
            "id": 22,
            "firstName": "Diego",
            "lastName": "Roel",
            "createdTimestamp": "2022-12-21T19:18:57.615060200"
        },
        "data-22": {
            "id": 23,
            "firstName": "Martine",
            "lastName": "Rancé",
            "createdTimestamp": "2022-12-21T19:18:57.615060200"
        },
        "data-23": {
            "id": 24,
            "firstName": "Maria",
            "lastName": "Larsson",
            "createdTimestamp": "2022-12-21T19:18:57.615060200"
        },
        "data-24": {
            "id": 25,
            "firstName": "Peter",
            "lastName": "Franken",
            "createdTimestamp": "2022-12-21T19:18:57.616054300"
        },
        "data-25": {
            "id": 26,
            "firstName": "Carine",
            "lastName": "Schmitt",
            "createdTimestamp": "2022-12-21T19:18:57.616054300"
        },
        "data-26": {
            "id": 27,
            "firstName": "Paolo",
            "lastName": "Accorti",
            "createdTimestamp": "2022-12-21T19:18:57.616054300"
        },
        "data-27": {
            "id": 28,
            "firstName": "Lino",
            "lastName": "Rodriguez",
            "createdTimestamp": "2022-12-21T19:18:57.616054300"
        },
        "data-28": {
            "id": 29,
            "firstName": "Eduardo",
            "lastName": "Saavedra",
            "createdTimestamp": "2022-12-21T19:18:57.617051400"
        },
        "data-29": {
            "id": 30,
            "firstName": "José",
            "lastName": "Pedro Freyre",
            "createdTimestamp": "2022-12-21T19:18:57.617051400"
        },
        "data-30": {
            "id": 31,
            "firstName": "André",
            "lastName": "Fonseca",
            "createdTimestamp": "2022-12-21T19:18:57.617051400"
        },
        "data-31": {
            "id": 32,
            "firstName": "Howard",
            "lastName": "Snyder",
            "createdTimestamp": "2022-12-21T19:18:57.618053600"
        },
        "data-32": {
            "id": 33,
            "firstName": "Manuel",
            "lastName": "Pereira",
            "createdTimestamp": "2022-12-21T19:18:57.618053600"
        },
        "data-33": {
            "id": 34,
            "firstName": "Mario",
            "lastName": "Pontes",
            "createdTimestamp": "2022-12-21T19:18:57.619053500"
        },
        "data-34": {
            "id": 35,
            "firstName": "Carlos",
            "lastName": "Hernández",
            "createdTimestamp": "2022-12-21T19:18:57.619053500"
        },
        "data-35": {
            "id": 36,
            "firstName": "Yoshi",
            "lastName": "Latimer",
            "createdTimestamp": "2022-12-21T19:18:57.619053500"
        },
        "data-36": {
            "id": 37,
            "firstName": "Patricia",
            "lastName": "McKenna",
            "createdTimestamp": "2022-12-21T19:18:57.619053500"
        },
        "data-37": {
            "id": 38,
            "firstName": "Helen",
            "lastName": "Bennett",
            "createdTimestamp": "2022-12-21T19:18:57.620054900"
        },
        "data-38": {
            "id": 39,
            "firstName": "Philip",
            "lastName": "Cramer",
            "createdTimestamp": "2022-12-21T19:18:57.620054900"
        },
        "data-39": {
            "id": 40,
            "firstName": "Daniel",
            "lastName": "Tonini",
            "createdTimestamp": "2022-12-21T19:18:57.620054900"
        },
        "data-40": {
            "id": 41,
            "firstName": "Annette",
            "lastName": "Roulet",
            "createdTimestamp": "2022-12-21T19:18:57.621046300"
        },
        "data-41": {
            "id": 42,
            "firstName": "Yoshi",
            "lastName": "Tannamuri",
            "createdTimestamp": "2022-12-21T19:18:57.621046300"
        },
        "data-42": {
            "id": 43,
            "firstName": "John",
            "lastName": "Steel",
            "createdTimestamp": "2022-12-21T19:18:57.621046300"
        },
        "data-43": {
            "id": 44,
            "firstName": "Renate",
            "lastName": "Messner",
            "createdTimestamp": "2022-12-21T19:18:57.622051800"
        },
        "data-44": {
            "id": 45,
            "firstName": "Jaime",
            "lastName": "Yorres",
            "createdTimestamp": "2022-12-21T19:18:57.622051800"
        },
        "data-45": {
            "id": 46,
            "firstName": "Carlos",
            "lastName": "González",
            "createdTimestamp": "2022-12-21T19:18:57.622051800"
        },
        "data-46": {
            "id": 47,
            "firstName": "Felipe",
            "lastName": "Izquierdo",
            "createdTimestamp": "2022-12-21T19:18:57.622051800"
        },
        "data-47": {
            "id": 48,
            "firstName": "Fran",
            "lastName": "Wilson",
            "createdTimestamp": "2022-12-21T19:18:57.623056"
        },
        "data-48": {
            "id": 49,
            "firstName": "Giovanni",
            "lastName": "Rovelli",
            "createdTimestamp": "2022-12-21T19:18:57.623056"
        },
        "data-49": {
            "id": 50,
            "firstName": "Catherine",
            "lastName": "Dewey",
            "createdTimestamp": "2022-12-21T19:18:57.623056"
        },
        "data-50": {
            "id": 51,
            "firstName": "Jean",
            "lastName": "Fresnière",
            "createdTimestamp": "2022-12-21T19:18:57.624059100"
        },
        "data-51": {
            "id": 52,
            "firstName": "Alexander",
            "lastName": "Feuer",
            "createdTimestamp": "2022-12-21T19:18:57.624059100"
        },
        "data-52": {
            "id": 53,
            "firstName": "Simon",
            "lastName": "Crowther",
            "createdTimestamp": "2022-12-21T19:18:57.624059100"
        },
        "data-53": {
            "id": 54,
            "firstName": "Yvonne",
            "lastName": "Moncada",
            "createdTimestamp": "2022-12-21T19:18:57.625046100"
        },
        "data-54": {
            "id": 55,
            "firstName": "Rene",
            "lastName": "Phillips",
            "createdTimestamp": "2022-12-21T19:18:57.625046100"
        },
        "data-55": {
            "id": 56,
            "firstName": "Henriette",
            "lastName": "Pfalzheim",
            "createdTimestamp": "2022-12-21T19:18:57.626050600"
        },
        "data-56": {
            "id": 57,
            "firstName": "Marie",
            "lastName": "Bertrand",
            "createdTimestamp": "2022-12-21T19:18:57.626050600"
        },
        "data-57": {
            "id": 58,
            "firstName": "Guillermo",
            "lastName": "Fernández",
            "createdTimestamp": "2022-12-21T19:18:57.627058400"
        },
        "data-58": {
            "id": 59,
            "firstName": "Georg",
            "lastName": "Pipps",
            "createdTimestamp": "2022-12-21T19:18:57.627058400"
        },
        "data-59": {
            "id": 60,
            "firstName": "Isabel",
            "lastName": "de Castro",
            "createdTimestamp": "2022-12-21T19:18:57.628051800"
        },
        "data-60": {
            "id": 61,
            "firstName": "Bernardo",
            "lastName": "Batista",
            "createdTimestamp": "2022-12-21T19:18:57.628051800"
        },
        "data-61": {
            "id": 62,
            "firstName": "Lúcia",
            "lastName": "Carvalho",
            "createdTimestamp": "2022-12-21T19:18:57.628051800"
        },
        "data-62": {
            "id": 63,
            "firstName": "Horst",
            "lastName": "Kloss",
            "createdTimestamp": "2022-12-21T19:18:57.629058300"
        },
        "data-63": {
            "id": 64,
            "firstName": "Sergio",
            "lastName": "Gutiérrez",
            "createdTimestamp": "2022-12-21T19:18:57.629058300"
        },
        "data-64": {
            "id": 65,
            "firstName": "Paula",
            "lastName": "Wilson",
            "createdTimestamp": "2022-12-21T19:18:57.630046800"
        },
        "data-65": {
            "id": 66,
            "firstName": "Maurizio",
            "lastName": "Moroni",
            "createdTimestamp": "2022-12-21T19:18:57.630046800"
        },
        "data-66": {
            "id": 67,
            "firstName": "Janete",
            "lastName": "Limeira",
            "createdTimestamp": "2022-12-21T19:18:57.630046800"
        },
        "data-67": {
            "id": 68,
            "firstName": "Michael",
            "lastName": "Holz",
            "createdTimestamp": "2022-12-21T19:18:57.630046800"
        },
        "data-68": {
            "id": 69,
            "firstName": "Alejandra",
            "lastName": "Camino",
            "createdTimestamp": "2022-12-21T19:18:57.631057200"
        },
        "data-69": {
            "id": 70,
            "firstName": "Jonas",
            "lastName": "Bergulfsen",
            "createdTimestamp": "2022-12-21T19:18:57.631057200"
        },
        "data-70": {
            "id": 71,
            "firstName": "Jose",
            "lastName": "Pavarotti",
            "createdTimestamp": "2022-12-21T19:18:57.631057200"
        },
        "data-71": {
            "id": 72,
            "firstName": "Hari",
            "lastName": "Kumar",
            "createdTimestamp": "2022-12-21T19:18:57.632051500"
        },
        "data-72": {
            "id": 73,
            "firstName": "Jytte",
            "lastName": "Petersen",
            "createdTimestamp": "2022-12-21T19:18:57.632051500"
        },
        "data-73": {
            "id": 74,
            "firstName": "Dominique",
            "lastName": "Perrier",
            "createdTimestamp": "2022-12-21T19:18:57.632051500"
        },
        "data-74": {
            "id": 75,
            "firstName": "Art",
            "lastName": "Braunschweiger",
            "createdTimestamp": "2022-12-21T19:18:57.632051500"
        },
        "data-75": {
            "id": 76,
            "firstName": "Pascale",
            "lastName": "Cartrain",
            "createdTimestamp": "2022-12-21T19:18:57.632051500"
        },
        "data-76": {
            "id": 77,
            "firstName": "Liz",
            "lastName": "Nixon",
            "createdTimestamp": "2022-12-21T19:18:57.632051500"
        },
        "data-77": {
            "id": 78,
            "firstName": "Liu",
            "lastName": "Wong",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-78": {
            "id": 79,
            "firstName": "Karin",
            "lastName": "Josephs",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-79": {
            "id": 80,
            "firstName": "Miguel",
            "lastName": "Angel Paolino",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-80": {
            "id": 81,
            "firstName": "Anabela",
            "lastName": "Domingues",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-81": {
            "id": 82,
            "firstName": "Helvetius",
            "lastName": "Nagy",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-82": {
            "id": 83,
            "firstName": "Palle",
            "lastName": "Ibsen",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-83": {
            "id": 84,
            "firstName": "Mary",
            "lastName": "Saveley",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-84": {
            "id": 85,
            "firstName": "Paul",
            "lastName": "Henriot",
            "createdTimestamp": "2022-12-21T19:18:57.633060200"
        },
        "data-85": {
            "id": 86,
            "firstName": "Rita",
            "lastName": "Müller",
            "createdTimestamp": "2022-12-21T19:18:57.634052200"
        },
        "data-86": {
            "id": 87,
            "firstName": "Pirkko",
            "lastName": "Koskitalo",
            "createdTimestamp": "2022-12-21T19:18:57.634052200"
        },
        "data-87": {
            "id": 88,
            "firstName": "Paula",
            "lastName": "Parente",
            "createdTimestamp": "2022-12-21T19:18:57.634052200"
        },
        "data-88": {
            "id": 89,
            "firstName": "Karl",
            "lastName": "Jablonski",
            "createdTimestamp": "2022-12-21T19:18:57.634052200"
        },
        "data-89": {
            "id": 90,
            "firstName": "Matti",
            "lastName": "Karttunen",
            "createdTimestamp": "2022-12-21T19:18:57.635056100"
        },
        "data-90": {
            "id": 91,
            "firstName": "Zbyszek",
            "lastName": "Piestrzeniewicz",
            "createdTimestamp": "2022-12-21T19:18:57.635056100"
        }
    }
"""

  private val jsonObjectSet = new JSONObject(jsonObjectData)
  (1 to n).foreach{_ =>
    val key = "data-" + new Random().nextInt(jsonObjectSet.length())
    jsonObjectSet.getJSONObject(key).put("createdTimestamp", java.time.LocalDateTime.now().toString)
    //    val topicKey = jsonObjectSet.getJSONObject(key).get("Country").toString
    val record = new ProducerRecord[String, String](topicName, null, jsonObjectSet.getJSONObject(key).toString())
    producer.send(record)
    Thread.sleep(timePeriod)
  }
  while(n<1){
    val key = "data-" + new Random().nextInt(jsonObjectSet.length())
    jsonObjectSet.getJSONObject(key).put("createdTimestamp", java.time.LocalDateTime.now().toString)
    val record = new ProducerRecord[String, String](topicName, null, jsonObjectSet.getJSONObject(key).toString())
    producer.send(record)
    Thread.sleep(timePeriod max 0)
  }
  producer.close()

}
object Producer{
  def main(args: Array[String]): Unit = {
    args.length match {
      case 2 if args(1).matches("\\d+") => new Producer(args(0),args(1).toInt)
      case 3 if args(1).matches("\\d+") && args(2).matches("\\d+") => new Producer(args(0),args(1).toInt,args(2).toInt)
      case _ => println(
        """
          |Wrong arguments!!
          |Producer(topicName:String,n:Int,timePeriod:Int)
          |topicName-> Name of the Kafka topic
          | n ->       number of messages you want to write
          |            for infinite messages, put n = 0 or neg.
          | timePeriod -> After how many seconds, next message will be written. By default, 100 milliseconds
          |""".stripMargin)
    }

  }
}
