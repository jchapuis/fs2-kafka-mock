package io.github.jchapuis.fs2.kafka.mock

import cats.Eq
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}
import cats.effect.std.Mutex
import cats.syntax.eq.*
import cats.syntax.traverse.*
import MockKafkaProducer.Patience
import fs2.kafka.producer.MkProducer
import fs2.kafka.*
import io.github.jchapuis.fs2.kafka.mock.impl.NativeMockKafkaProducer
import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Defines methods for a mock kafka producer: allows for checking for test messages and provides a MkProducer instance.
  * Keeps an internal offset for each topic so that messages can be consumed one by one with convenience methods.
  */
trait MockKafkaProducer {

  /** Returns the list of published messages for the given topic.
    * @param topic
    *   the topic to get the history for
    * @param keyDeserializer
    *   the key deserializer
    * @param valueDeserializer
    *   the value deserializer
    * @tparam K
    *   the key type
    * @tparam V
    *   the value type
    * @return
    *   the list of published messages thus far
    */
  def historyFor[K, V](topic: String)(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[List[(K, V)]]

  /** Returns the list of published messages for the given topic and key.
    * @param topic
    *   the topic to get the history for
    * @param key
    *   the key to get the history for
    * @param keyDeserializer
    *   the key deserializer
    * @param valueDeserializer
    *   the value deserializer
    * @tparam K
    *   the key type
    * @tparam V
    *   the value type
    * @return
    *   the list of published messages thus far
    */
  def historyFor[K: Eq, V](topic: String, key: K)(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[List[V]]

  /** Returns the next message for the given topic, if any. Increments the internal offset for the topic.
    * @param topic
    *   the topic to get the next message for
    * @param keyDeserializer
    *   the key deserializer
    * @param valueDeserializer
    *   the value deserializer
    * @tparam K
    *   the key type
    * @tparam V
    *   the value type
    * @return
    *   the next message for the given topic, if any
    */
  def nextMessageFor[K, V](topic: String)(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[Option[(K, V)]]

  /** Returns the next message for the given topic. Semantically blocks, with polling intervals and timeout specified
    * with the patience implicit parameter
    * @param topic
    *   the topic to get the next message for
    * @param patience
    *   the patience to use for polling for the next message
    * @param keyDeserializer
    *   the key deserializer
    * @param valueDeserializer
    *   the value deserializer
    * @throws `NoSuchElementException`
    *   if no message is available before the timeout
    */
  def nextEventualMessageFor[K, V](topic: String)(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[(K, V)]

  /** Returns the next message for the given topic and key, if any. Increments the internal offset for the topic.
    * @param topic
    *   the topic to get the next message for
    * @param key
    *   the key to get the next message for
    * @param keyDeserializer
    *   the key deserializer
    * @param valueDeserializer
    *   the value deserializer
    * @tparam K
    *   the key type
    * @tparam V
    *   the value type
    * @return
    *   the next message for the given topic and key, if any
    */
  def nextValueFor[K: Eq, V](topic: String, key: K)(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[Option[V]]

  /** Returns the next message for the given topic and key. Semantically blocks, with polling intervals and timeout
    * specified with the patience implicit parameter
    * @param topic
    *   the topic to get the next message for
    * @param key
    *   the key to get the next message for
    * @param patience
    *   the patience to use for polling for the next message
    * @param keyDeserializer
    *   the key deserializer
    * @param valueDeserializer
    *   the value deserializer
    * @throws `NoSuchElementException`
    *   if no message is available before the timeout
    */
  def nextEventualValueFor[K: Eq, V](topic: String, key: K)(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[V]

  /** Returns the next message for the given topic and key wrapped in Some, or None if a redaction was received.
    * Semantically blocks, with polling intervals and timeout specified with the patience implicit parameter
    * @param topic
    *   the topic to get the next message for
    * @param key
    *   the key to get the next message for
    * @param patience
    *   the patience to use for polling for the next message
    * @param keyDeserializer
    *   the key deserializer
    * @param valueDeserializer
    *   the value deserializer
    * @throws `NoSuchElementException`
    *   if no message is available before the timeout
    */
  def nextEventualValueOrRedactionFor[K: Eq, V](topic: String, key: K)(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[Option[V]]

  /** MkProducer instance providing the mock producer. Including this instance in implicit scope where the kafka
    * producer is created will feed it with the mock producer instance instead of the real one
    */
  implicit def mkProducer: MkProducer[IO]
}

object MockKafkaProducer {
  final case class Patience(timeout: FiniteDuration, interval: FiniteDuration)
  object Patience {
    implicit val default: Patience = Patience(150.millis, 15.millis)
  }

  /** Creates a mock kafka producer, backed by the mock producer built into the kafka client library.
    * @return
    *   a resource containing the mock kafka producer
    */
  def apply(): Resource[IO, MockKafkaProducer] =
    Resource
      .eval(Ref.of[IO, Map[String, Int]](Map.empty))
      .flatMap(currentOffsets =>
        Resource
          .eval(Mutex[IO])
          .map(mutex =>
            new NativeMockKafkaProducer(
              new MockProducer[Array[Byte], Array[Byte]](true, new ByteArraySerializer, new ByteArraySerializer),
              currentOffsets,
              mutex
            )
          )
      )
}
