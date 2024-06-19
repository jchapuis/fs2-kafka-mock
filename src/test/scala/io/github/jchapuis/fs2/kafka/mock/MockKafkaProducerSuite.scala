package io.github.jchapuis.fs2.kafka.mock

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.eq.*
import fs2.Chunk
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerSettings}
import fs2.kafka.producer.MkProducer
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class MockKafkaProducerSuite extends CatsEffectSuite {
  val topic = "test-topic"
  val key = "test-key"
  val value = "test-value"
  val record = ProducerRecord[String, String](topic, key, value)

  test("mock kafka producer returns the history of published messages") {
    createMockProducer.use { case (mock, producer) =>
      for {
        _ <- producer.produce(ProducerRecords(Chunk(record, record, record))).flatten
        topicHistory <- mock.historyFor[String, String](topic)
        _ <- IO(topicHistory.size == 3).assert
        _ <- IO(topicHistory.forall { case (key, value) => key === key && value === value }).assert
        keyHistory <- mock.historyFor[String, String](topic, key)
        _ <- IO(keyHistory.size == 3).assert
        _ <- IO(keyHistory.forall(_ === value)).assert
      } yield ()
    }
  }

  test("mock kafka producer returns next messages iteratively") {
    createMockProducer.use { case (mock, producer) =>
      for {
        _ <- producer.produce(ProducerRecords.one(record)).flatten
        _ <- mock
          .nextMessageFor[String, String](topic)
          .map(maybeKeyValue => assertEquals(maybeKeyValue, Some((key, value))))
        _ <- producer
          .produce(ProducerRecords(Chunk(ProducerRecord(topic, key, "foo"), ProducerRecord(topic, key, "bar"))))
          .flatten
        _ <- mock.nextValueFor[String, String](topic, key).map(assertEquals(_, Some("foo")))
        _ <- mock.nextValueFor[String, String](topic, key).map(assertEquals(_, Some("bar")))
      } yield ()
    }
  }

  test("mock kafka producer supports detecting redaction") {
    createMockProducer.use { case (mock, producer) =>
      createMockRedactor(mock.mkProducer).use { redactor =>
        for {
          _ <- producer.produce(ProducerRecords.one(record)).flatten
          _ <- mock
            .nextMessageFor[String, String](topic)
            .map(maybeKeyValue => assertEquals(maybeKeyValue, Some((key, value))))
          _ <- redactor.produceOne(topic, key, ())
          _ <- mock.nextEventualValueOrRedactionFor[String, String](topic, key).map(assertEquals(_, None)) // redaction
          _ <- producer.produce(ProducerRecords.one(ProducerRecord(topic, key, "foo"))).flatten
          _ <- mock.nextEventualValueOrRedactionFor[String, String](topic, key).map(assertEquals(_, Some("foo")))
          _ <- mock.nextValueFor[String, String](topic, key).map(assertEquals(_, None))

        } yield ()
      }
    }
  }

  test("mock kafka producer returns eventual message") {
    implicit val patience = MockKafkaProducer.Patience(timeout = 2.seconds, interval = 100.millis)
    val test = createMockProducer.use { case (mock, producer) =>
      for {
        _ <- {
          IO.sleep(1.second) >>
            producer.produce(ProducerRecords.one(record)).flatten >>
            IO.sleep(1.second) >>
            producer.produce(ProducerRecords.one(record)).flatten
        }.start
        _ <- mock
          .nextEventualMessageFor[String, String](topic)
          .map(record => assertEquals(record, (key, value)))
        _ <- mock.nextEventualValueFor[String, String](topic, key).map(assertEquals(_, value))
      } yield ()
    }
    TestControl.executeEmbed(test)
  }

  test("mock kafka producer raises exception when no eventual message") {
    implicit val patience = MockKafkaProducer.Patience(timeout = 1.seconds, interval = 100.millis)
    val test = createMockProducer.use { case (mock, producer) =>
      for {
        _ <- { IO.sleep(2.second) >> producer.produce(ProducerRecords.one(record)).flatten }.start
        _ <- mock
          .nextEventualMessageFor[String, String](topic)
          .map(assertEquals(_, (key, value)))
      } yield ()
    }
    TestControl.executeEmbed(test).intercept[NoSuchElementException]
  }

  private def createMockProducer = {
    MockKafkaProducer()
      .flatMap { mock =>
        implicit val mkProducer: MkProducer[IO] = mock.mkProducer
        val producer = KafkaProducer.resource(ProducerSettings[IO, String, String])
        producer.map((mock, _))
      }
  }

  private def createMockRedactor(implicit mkProducer: MkProducer[IO]) =
    KafkaProducer.resource(ProducerSettings[IO, String, Unit])

  // README.md example
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
}
