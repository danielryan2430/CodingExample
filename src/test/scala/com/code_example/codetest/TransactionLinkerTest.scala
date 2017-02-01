package com.code_example.codetest

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by danielimberman on 29/01/17.
  */
class TransactionLinkerTest extends FlatSpec with Matchers{
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("coding test"))
  val eventStrings = Seq(
    "babbe8fe-5dd0-4c53-b1ea-4826095ac925,2016-01-01T01:30:55.000+00:00,fee830db7fd1554b,/blades",
    "d136e575-528e-45d6-8922-e6222b14dfae,2016-01-01T01:31:32.000+00:00,fee830db7fd1554b,/your-account",
    "3ccf2fa0-9ac6-4f38-ab06-e01e5b358b07,2016-01-01T04:30:55.000+00:00,fee830db7fd1554b,/blades",
    "d171059c-32c2-418b-a2f2-63da45b60b27,2016-01-01T04:30:56.000+00:00,e2112d7f2b57fbef,/our-products",
    "d2f33627-ad2a-4e9b-8d68-1320cd8d600e,2016-01-01T04:31:07.000+00:00,e2112d7f2b57fbef,/your-account")
  
  
  val largeEventStringList = Seq("babbe8fe-5dd0-4c53-b1ea-4826095ac925,2016-01-01T01:30:55.000+00:00,fee830db7fd1554b,/blades",
"d136e575-528e-45d6-8922-e6222b14dfae,2016-01-01T01:31:32.000+00:00,3ff4b08d281b4bdb,/your-account",
"3ccf2fa0-9ac6-4f38-ab06-e01e5b358b07,2016-01-01T04:30:55.000+00:00,7948dc04202b272b,/blades",
"d171059c-32c2-418b-a2f2-63da45b60b27,2016-01-01T04:30:56.000+00:00,e2112d7f2b57fbef,/our-products",
"d2f33627-ad2a-4e9b-8d68-1320cd8d600e,2016-01-01T04:31:07.000+00:00,e2112d7f2b57fbef,/your-account",
"92bf2b9d-f785-4503-90f1-784e2aec398a,2016-01-01T04:31:35.000+00:00,e2112d7f2b57fbef,/your-box/item/MNgCneCQnBTsAhpUSKW9lFF4HVq0-nvtRoKs8OI38JVMG7T9WUh0qGvoWr0ApzXz/change-frequency",
"240266ab-1abd-4d3f-8482-d8743b0fd6a5,2016-01-01T04:31:51.000+00:00,e2112d7f2b57fbef,/our-products/fresh/travel-wipes",
"76a70711-9e39-4aae-8f0b-8c6786b4102b,2016-01-01T04:32:56.000+00:00,bef47dbdecd96b89,/your-box/add-product/post-shave/qty/1",
"b12d337c-4933-4a4e-899d-45ab6845df4f,2016-01-01T04:33:00.000+00:00,ecac855fedd77e5f,/your-box/add-product/wipes/qty/1",
"7f2758a9-9aab-44bb-90f4-08f6049d0a70,2016-01-01T04:35:54.000+00:00,705e64359c4a39cb,/our-products",
"a8f8f7d4-86df-4a54-9606-ecb35d669b3e,2016-01-01T05:30:57.000+00:00,c64802b1ca172cf8,/our-products/style/match/hold",
"6a7a011e-b11f-4c01-970c-07dc8dc5e420,2016-01-01T05:31:04.000+00:00,c64802b1ca172cf8,/our-products/style/match",
"8cbd37d3-ba69-43ce-8833-bcf879bc88b5,2016-01-01T05:31:48.000+00:00,b3472db73c1d1127,/your-box/add-product/2-blade-razor-handle/qty/1",
"eed05b43-e36a-4b1f-9baa-e5022d348d73,2016-01-01T05:36:09.000+00:00,85561c21318be2df,/checkout/extras",
"b2553519-80cf-4868-baf0-885b14ddb193,2016-01-01T13:31:32.000+00:00,6bbd4bd0f7838698,/our-products/fresh",
"f86b63c3-18cb-49db-b6f4-5ee275b1df9b,2016-01-01T13:31:36.000+00:00,6bbd4bd0f7838698,/our-products/style",
"080745a2-e11b-42d3-8ad8-18986c9dfe51,2016-01-01T13:34:15.000+00:00,bfc165df9d6850d8,/checkout/complete",
"5a8eacae-0e79-4e20-823a-3ebfbc2622d3,2016-01-01T17:31:16.000+00:00,eb79e588d3388150,/our-products",
"d815a601-d6d8-41e7-a2b2-d2192be48966,2016-01-01T17:31:31.000+00:00,eb79e588d3388150,/our-products/humble-twin",
"a2cecb3c-f998-4ec7-b388-c9abc03ea4ae,2016-01-01T17:36:09.000+00:00,c2dfe200fe5e0367,/your-box/next",
"29596dbc-f333-49a1-a59c-1893da3036c0,2016-01-01T20:32:08.000+00:00,6ad0253cb188dc3a,/",
"e4e7530b-7a11-4c77-8dd9-cb853546f0f2,2016-01-01T20:33:06.000+00:00,0eb66eb646f99991,/login",
"bb7b1fbe-b0af-4413-81ec-48d2b6857d87,2016-01-01T23:30:48.000+00:00,52b05aa60e10999f,/",
"d9c6fa46-2d5a-4089-b33d-2d1332564623,2016-01-01T23:31:53.000+00:00,f147b02e7c8aa769,/our-products/fresh/travel-wipes",
"ec5f239e-6405-47fb-93f1-5e2e9a621cb7,2016-01-01T23:32:41.000+00:00,6a58e985a6e24549,/your-box/add-product/wipes",
"1bd39237-d501-4f31-b4c1-a535ecf08e40,2016-01-01T23:32:45.000+00:00,6a58e985a6e24549,/our-products",
"044bc35c-8392-4b6d-8ee0-d1ac04060ed9,2016-01-01T23:34:18.000+00:00,53a0ed1ab0cd6704,/your-box/now/checkout",
"ccc55a63-218a-4d0d-98e5-7f3b1b463e62,2016-01-02T01:31:07.000+00:00,3b13cdb10151c843,/your-account",
"6d43da38-2cb6-46eb-8e80-7e1be93e7680,2016-01-02T01:31:25.000+00:00,ecd130a96ce740f0,/our-products",
"94046699-829d-40bd-9808-113044a64e99,2016-01-02T01:31:49.000+00:00,08740e00d3925e9d,/our-products",
"f9cd77d4-3b5d-4443-9a57-c120320048c6,2016-01-02T01:32:03.000+00:00,44c6fd991e406d05,/your-box/now/preview",
"5caae22a-25c9-46cd-82e2-8c6271f43d49,2016-01-02T01:35:25.000+00:00,25a0d2aa81a1da3c,/your-box/funnel",
"09b39708-cd2b-47df-9c01-a3ff94ce3a6e,2016-01-02T01:37:02.000+00:00,037497c898457a34,/checkout/extras",
"175ea29a-b0a9-4b1c-936b-627ef428002b,2016-01-02T04:30:41.000+00:00,95f7c214a71a0fbc,/login",
"8b2939a4-3fcb-48f0-9ad2-80c9d692f2e2,2016-01-02T04:31:06.000+00:00,6ef8b8bac1e13fec,/our-products",
"21168671-88cb-4a8c-b6ff-909f6dfc2630,2016-01-02T04:32:01.000+00:00,91a7f575087a5f67,/your-account",
"92f42e8e-5459-4c51-a78c-7e4284322ff1,2016-01-02T04:32:21.000+00:00,e384435264d4d486,/our-products/style/match/current-product",
"ea2b3327-8d0d-4ae6-90c1-721b6cafae2d,2016-01-02T04:33:08.000+00:00,9d1a5de99b5983a6,/your-box/add-product/post-shave",
"1d53b75e-b95c-4ec6-ba40-4bf00de7f2d9,2016-01-02T04:35:00.000+00:00,ca029ccf3b131cef,/your-box/add-product/hair-fiber",
"f7e8e3cf-069a-4f7d-b32e-c4827add1996,2016-01-02T04:35:07.000+00:00,7415cea5eddfa288,/checkout/extras",
"a8062dd2-a0c2-4404-86b9-1ca31ce172e7,2016-01-02T04:35:40.000+00:00,c5f2f32873e8e100,/your-box/funnel")
  
  
  

  val incorrectEventStrings = Seq("2016-01-01T01:30:55.000+00:00,fee830db7fd1554b,/blades",
  "d136e575-528e-45d6-8922-e6222b14dfae,2016-01-01T01:3a1:32.000+00:00,3ff4b08d281b4bdb,/your-account",
  "3ccf2fa0-9ac6-4f38-ab06-e01e5b358b07,2016-01-01T04:30:55.000+00:00,7948dc04202b272b")


  "parseUsersFromCSV" should "convert an RDD of string an RDD of (userId, user)" in {
    val userRDD = sc.parallelize(eventStrings)

    val x = TransactionLinker.parseUsersFromCSV(userRDD).collect()
    x.length shouldEqual 5

    val events = x.map(_._2)
    events.head shouldEqual Transaction("babbe8fe-5dd0-4c53-b1ea-4826095ac925",
      Transaction.parseDate("2016-01-01T01:30:55.000+00:00"),
      "fee830db7fd1554b",
      "/blades",
      "")
    events(1) shouldEqual Transaction(
      "d136e575-528e-45d6-8922-e6222b14dfae",
      Transaction.parseDate("2016-01-01T01:31:32.000+00:00"),
      "fee830db7fd1554b",
      "/your-account",
      "")
  }

  it should "filter out incorrect values" in {
    val userRDD = sc.parallelize(eventStrings ++ incorrectEventStrings)
    val events = TransactionLinker.linkTransactionsToNextTimestamp(userRDD).collect()

    events.length shouldEqual 5

    events.head shouldEqual "3ccf2fa0-9ac6-4f38-ab06-e01e5b358b07,2016-01-01T04:30:55.000+00:00,fee830db7fd1554b,/blades,"
    events(1) shouldEqual "d136e575-528e-45d6-8922-e6222b14dfae,2016-01-01T01:31:32.000+00:00,fee830db7fd1554b,/your-account,3ccf2fa0-9ac6-4f38-ab06-e01e5b358b07"
  }

  "getTimeStamps" should "write timestamps for each user" in {
    val userRDD = sc.parallelize(largeEventStringList)
    val answer = TransactionLinker.linkTransactionsToNextTimestamp(userRDD).collect().toList.sorted
    answer.foreach(println)

    answer.length shouldEqual largeEventStringList.length

    val releventInfo = answer.map(_.split(",")).map(t => (t(0), Transaction.parseDate(t(1)),Transaction.parseNextId(t.toList)))

    val eventMap = releventInfo.map{case(event, date, next) => (event,date)}.toMap

    releventInfo.foreach{
      case(event,date,next) =>
        if(next != "")  date.compareTo(eventMap(next)) shouldEqual -1
    }

  }

}
