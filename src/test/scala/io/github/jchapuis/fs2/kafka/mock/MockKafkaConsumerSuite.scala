package io.github.jchapuis.fs2.kafka.mock

import cats.effect.IO
import cats.syntax.traverse.*
import cats.syntax.eq.*
import fs2.kafka.{ConsumerSettings, KafkaConsumer, Timestamp}
import fs2.kafka.consumer.MkConsumer
import fs2.Stream
import munit.CatsEffectSuite

class MockKafkaConsumerSuite extends CatsEffectSuite {
  val topicA = "test-topic-a"
  val keyA = "test-key-a"
  val valueA = "test-value-a"
  val topicB = "test-topic-b"
  val keyB = "test-key-b"
  val valueB = "test-value-b"

  test("mock kafka consumer publishes messages that can be read") {
    MockKafkaConsumer(topicA, topicB).use { mockConsumer =>
      for {
        _ <- (mockConsumer.publish(topicA, keyA, valueA) >>
          mockConsumer.publish(topicB, keyB, valueB) >>
          mockConsumer.publish(topicA, keyB, valueB)).start
        records <- {
          implicit val mkConsumer: MkConsumer[IO] = mockConsumer.mkConsumer
          KafkaConsumer
            .stream(ConsumerSettings[IO, String, String].withGroupId("test"))
            .subscribeTo(topicA, topicB)
            .records
            .map(_.record)
            .map(record => (record.topic, record.key, record.value))
            .take(3)
            .compile
            .toList
        }
        _ <- IO(records.contains((topicA, keyA, valueA))).assert
        _ <- IO(records.contains((topicA, keyB, valueB))).assert
        _ <- IO(records.contains((topicB, keyB, valueB))).assert
      } yield ()
    }
  }

  test("mock kafka consumer supports specifying record timestamp") {
    MockKafkaConsumer(topicA).use { mockConsumer =>
      for {
        timestamp2 <- IO.realTimeInstant
        timestamp3 <- IO.realTimeInstant
        timestamp1 <- IO.realTimeInstant
        _ <- (mockConsumer.publish(topicA, keyA, valueA, Some(timestamp1)) >>
          mockConsumer.publish(topicA, keyA, valueA, Some(timestamp2)) >>
          mockConsumer.publish(topicA, keyA, valueA, Some(timestamp3))).start
        assertions <- {
          implicit val mkConsumer: MkConsumer[IO] = mockConsumer.mkConsumer
          KafkaConsumer
            .stream(ConsumerSettings[IO, String, String].withGroupId("test"))
            .subscribeTo(topicA)
            .records
            .map(_.record.timestamp)
            .take(3)
            .zipWith(Stream(timestamp1, timestamp2, timestamp3)) { case (obtained, expected) =>
              IO(obtained === Timestamp.createTime(expected.toEpochMilli)).assert
            }
            .compile
            .toList
        }
        _ <- assertions.sequence
      } yield ()
    }
  }

  test("mock kafka consumer supports redacting entries") {
    MockKafkaConsumer(topicA).use { mockConsumer =>
      for {
        _ <- (mockConsumer.publish(topicA, keyA, valueA) >> mockConsumer.redact(topicA, keyA)).start
        records <- {
          implicit val mkConsumer: MkConsumer[IO] = mockConsumer.mkConsumer
          KafkaConsumer
            .stream(ConsumerSettings[IO, String, Option[String]].withGroupId("test"))
            .subscribeTo(topicA)
            .records
            .map(_.record)
            .take(2)
            .compile
            .toList
        }
        _ <- IO(records.headOption.flatMap(_.value)).assertEquals(Some(valueA))
        _ <- IO(records.lastOption.flatMap(_.value)).assertEquals(None)
      } yield ()
    }
  }

  test("mock kafka consumer handles Option values correctly") {
    MockKafkaConsumer(topicA).use { mockConsumer =>
      for {
        _ <- mockConsumer.publish[String, Option[String]](topicA, keyA, None).start
        records <- {
          implicit val mkConsumer: MkConsumer[IO] = mockConsumer.mkConsumer
          KafkaConsumer
            .stream(ConsumerSettings[IO, String, Option[String]].withGroupId("test"))
            .subscribeTo(topicA)
            .records
            .map(_.record)
            .take(1)
            .compile
            .toList
        }
        _ <- IO(records.headOption.flatMap(_.value)).assertEquals(None)
      } yield ()
    }
  }

  // README.md example
  test("mock kafka consumer can read a published message") {
    MockKafkaConsumer("topic").use { mockConsumer =>
      for {
        _ <- mockConsumer
          .publish("topic", "key", "value")
          .start // this call semantically blocks until we can publish to the consumer, so launch it into a fiber
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
}
