# fs2-kafka-mock
[![Release](https://github.com/jchapuis/fs2-kafka-mock/actions/workflows/release.yml/badge.svg)](https://github.com/jchapuis/fs2-kafka-mock/actions/workflows/release.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.jchapuis/fs2-kafka-mock_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.jchapuis/fs2-kafka-mock_2.13)
[![codecov](https://codecov.io/gh/jchapuis/fs2-kafka-mock/branch/master/graph/badge.svg?token=BOAOIFC7BF)](https://codecov.io/gh/jchapuis/fs2-kafka-mock)
<a href="https://typelevel.org/cats/"><img src="https://typelevel.org/cats/img/cats-badge.svg" height="40px" align="right" alt="Cats friendly" /></a>

Mocks for fs2-kafka consumers and producers wrapping the native mocks built into the apache kafka clients library. This allows for testing applications without the need for a real kafka implementation such as testcontainers or embedded kafka. 

## Usage

Add the following dependency to your `build.sbt`:

```scala
libraryDependencies += "io.github.jchapuis" %% "fs2-kafka-mock" % "{latest version}"
```

## Mock consumer
The [mock consumer](src/main/scala/io/github/jchapuis/fs2/kafka/mock/MockKafkaConsumer.scala) allows covering code making use of fs2-kafka's `KafkaConsumer`. Injection of the mock is done via the implicit `MkConsumer` parameter (that fs2-kafka had the foresight to allow). Methods on the mock allow for publishing and redacting messages, as if these were being published on the real kafka. 

Internally, the mock consumer tracks published records in an array. Note, however, that there are some limitations imposed by the native kafka mock: 
- single partition support
- consumers must subscribe for publication to succeed

### Example
Here's a short example taken from the project's munit test suite: we mock publication of a message, consume it using a fs2 kafka consumer configured with the mock and verify that the consumer was able to read the record.

```scala
test("mock kafka consumer can read a published message") {
  MockKafkaConsumer("topic").use { mockConsumer =>
    for {
      _ <- mockConsumer
        .publish("topic", "key", "value")
        .start // this call semantically blocks until we can publish to the consumer
               // hence the need to run it in a separate fiber
      record <- {
        implicit val mkConsumer: MkConsumer[IO] = mockConsumer.mkConsumer
        KafkaConsumer
          .stream(
            ConsumerSettings[IO, String, String]
              .withGroupId("test")
          )
          .subscribeTo("topic")
          .records
          .map(_.record)
          .map(record => (record.topic, record.key, record.value))
          .take(1)
          .compile
          .toList
          .map(_.head)
      }
    } yield assertEquals(record, ("topic", "key", "value"))
  }
}
```

## Mock producer
The [mock producer](src/main/scala/io/github/jchapuis/fs2/kafka/mock/MockKafkaProducer.scala) allows covering code producing with fs2-kafka's `KafkaProducer`. Injection of the mock is done via the implicit `MkProducer` parameter, in a similar way as for the consumer. Various access methods on the mock allow for retrieving published records and iteratively checking for newer messages.

```scala
test("mock kafka producer returns next message and allows for checking full history") {
  MockKafkaProducer()
  .flatMap { mock =>
    implicit val mkProducer: MkProducer[IO] = mock.mkProducer
    val producer = KafkaProducer.resource(ProducerSettings[IO, String, String])
    producer.map((mock, _))
  }
  .use { case (mock, producer) =>
    for {
      _ <- producer.produce(ProducerRecords.one(ProducerRecord[String, String]("topic", "key", "value"))).flatten
      _ <- mock
        .nextMessageFor[String, String]("topic")
        .map(maybeKeyValue => assertEquals(maybeKeyValue, Some(("key", "value"))))
      _ <- mock
        .historyFor[String, String]("topic")
        .map(history => assertEquals(history, List(("key", "value"))))
    } yield ()
  }
}
```

