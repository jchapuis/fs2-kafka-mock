package io.github.jchapuis.fs2.kafka.mock

import cats.effect.kernel.Ref
import cats.effect.std.Mutex
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import fs2.kafka.*
import fs2.kafka.consumer.MkConsumer
import io.github.jchapuis.fs2.kafka.mock.impl.NativeMockKafkaConsumer
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition

import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Defines methods for a mock kafka consumer: allows for publishing test messages and provides a MkConsumer instance
  */
trait MockKafkaConsumer {

  /** Publish a message to the topic.
    *
    * Semantically blocking until a consumer subscribes to the topic: internally, it's polling for assignments, as the
    * native kafka mock doesn't support upfront publication but requires an assignment to be made before publishing.
    * @param topic
    *   the topic to publish to
    * @param key
    *   the key to publish
    * @param value
    *   the value to publish
    * @param timestamp
    *   optional timestamp of the message
    * @param keySerializer
    *   the key serializer
    * @param valueSerializer
    *   the value serializer
    * @tparam K
    *   the key type
    * @tparam V
    *   the value type
    * @return
    *   once published, a unit
    */
  def publish[K, V](topic: String, key: K, value: V, timestamp: Option[Instant] = None)(implicit
      keySerializer: KeySerializer[IO, K],
      valueSerializer: ValueSerializer[IO, V]
  ): IO[Unit]

  /** Redact a message from the topic, aka. publish a tombstone.
    *
    * @param topic
    *   the topic to redact from
    * @param key
    *   the key to redact
    * @param keySerializer
    *   the key serializer
    * @tparam K
    *   the key type
    * @return
    *   once redacted, a unit
    */
  def redact[K](topic: String, key: K)(implicit keySerializer: KeySerializer[IO, K]): IO[Unit]

  /** MkConsumer instance providing the mock consumer. Including this instance in implicit scope where the kafka
    * consumer resource is created will feed it with the mock consumer instance instead of the default, real one.
    */
  implicit def mkConsumer: MkConsumer[IO]
}

object MockKafkaConsumer {

  /** Create a mock kafka consumer for the given topics. Backed by the mock consumer built into the kafka client
    * library.
    *
    * @param topics
    *   the topics to create the consumer for
    * @param IORuntime
    *   the implicit IORuntime
    * @return
    *   a resource containing the mock kafka consumer
    */
  def apply(topics: String*)(implicit IORuntime: IORuntime): Resource[IO, MockKafkaConsumer] =
    Resource.eval(Ref.of[IO, Map[String, Long]](topics.map(_ -> 0L).toMap)).flatMap { currentOffsets =>
      val mockConsumer = new MockConsumer[Array[Byte], Array[Byte]](OffsetResetStrategy.EARLIEST)
      Resource.make(Mutex[IO].map { mutex =>
        val partitions = topics.map(topic => new TopicPartition(topic, 0))
        val beginningOffsets = partitions.map(_ -> (0L: java.lang.Long)).toMap
        mockConsumer.updateBeginningOffsets(beginningOffsets.asJava)
        new NativeMockKafkaConsumer(mockConsumer, currentOffsets, mutex)
      })(_ => IO(mockConsumer.close()))
    }

}
