package io.github.jchapuis.fs2.kafka.mock.impl

import cats.Eq
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Mutex
import cats.syntax.eq.*
import cats.syntax.traverse.*
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import io.github.jchapuis.fs2.kafka.mock.MockKafkaProducer
import io.github.jchapuis.fs2.kafka.mock.MockKafkaProducer.Patience
import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}

import scala.jdk.CollectionConverters.*

private[mock] class NativeMockKafkaProducer(
    val mockProducer: MockProducer[Array[Byte], Array[Byte]],
    currentOffsets: Ref[IO, Map[String, Int]],
    mutex: Mutex[IO]
) extends MockKafkaProducer {

  def nextMessageFor[K, V](topic: String)(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[Option[(K, V)]] =
    nextSelectedRecord(topic, record => IO(record.topic === topic)).map(_.collect { case (k, Some(v)) => (k, v) })

  def nextValueFor[K: Eq, V](topic: String, key: K)(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[Option[V]] =
    nextSelectedRecord(
      topic,
      record =>
        if (record.topic === topic) keyDeserializer.deserialize(topic, Headers.empty, record.key).map(_ === key)
        else IO.pure(false)
    ).map(_.collect { case (_, Some(value)) => value })

  private def nextSelectedRecord[K, V](
      topic: String,
      recordSelector: ProducerRecord[Array[Byte], Array[Byte]] => IO[Boolean]
  )(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[Option[(K, Option[V])]] =
    mutex.lock.surround {
      for {
        currentOffset <- currentOffsets.get.map(_.getOrElse(topic, -1))
        messages <- selectedHistory[K, V](topic, recordSelector)
        maybeNextRecord = messages
          .drop(messages.indexWhere { case (index, _, _) => index === currentOffset } + 1)
          .headOption
        _ <- IO.whenA(maybeNextRecord.isDefined)(
          currentOffsets.update(_.updated(topic, maybeNextRecord.map { case (index, _, _) => index }.get))
        )
      } yield maybeNextRecord.map { case (_, key, value) => (key, value) }
    }

  def nextEventualMessageFor[K, V](
      topic: String
  )(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[(K, V)] = nextEventualRecordFor[K, V](topic, record => IO(record.topic === topic))

  def nextEventualValueFor[K: Eq, V](topic: String, key: K)(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[V] = nextEventualRecordFor[K, V](
    topic,
    record =>
      if (record.topic === topic) keyDeserializer.deserialize(topic, Headers.empty, record.key).map(_ === key)
      else IO.pure(false)
  ).map { case (_, value) => value }

  def nextEventualValueOrRedactionFor[K: Eq, V](topic: String, key: K)(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[Option[V]] = nextEventualRecordOrRedactedFor[K, V](
    topic,
    record =>
      if (record.topic === topic) keyDeserializer.deserialize(topic, Headers.empty, record.key).map(_ === key)
      else IO.pure(false)
  ).map { case (_, value) => value }

  // note that this is not tail-recursive
  private def nextEventualRecordFor[K, V](
      topic: String,
      recordSelector: ProducerRecord[Array[Byte], Array[Byte]] => IO[Boolean]
  )(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[(K, V)] = nextSelectedRecord[K, V](topic, recordSelector).flatMap {
    case Some((k, Some(v))) => IO.pure((k, v))
    case _ if patience.timeout.toNanos > 0 =>
      IO.sleep(patience.interval) *> {
        val nextPatience: Patience = patience.copy(timeout = patience.timeout - patience.interval)
        nextEventualRecordFor[K, V](topic, recordSelector)(nextPatience, implicitly, implicitly)
      }
    case _ => IO.raiseError(new NoSuchElementException(s"no message found for topic $topic"))
  }

  // note that this is not tail-recursive
  private def nextEventualRecordOrRedactedFor[K, V](
      topic: String,
      recordSelector: ProducerRecord[Array[Byte], Array[Byte]] => IO[Boolean]
  )(implicit
      patience: Patience,
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[(K, Option[V])] = nextSelectedRecord[K, V](topic, recordSelector).flatMap {
    case Some(record) => IO.pure(record)
    case None if patience.timeout.toNanos > 0 =>
      IO.sleep(patience.interval) *> {
        val nextPatience: Patience = patience.copy(timeout = patience.timeout - patience.interval)
        nextEventualRecordOrRedactedFor[K, V](topic, recordSelector)(nextPatience, implicitly, implicitly)
      }
    case None => IO.raiseError(new NoSuchElementException(s"no message found for topic $topic"))
  }
  def historyFor[K, V](
      topic: String
  )(implicit keyDeserializer: KeyDeserializer[IO, K], valueDeserializer: ValueDeserializer[IO, V]): IO[List[(K, V)]] =
    selectedHistory(topic, record => IO(record.topic === topic)).map(_.collect { case (_, key, Some(value)) =>
      (key, value)
    })

  def historyFor[K: Eq, V](topic: String, key: K)(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[List[V]] = selectedHistory[K, V](
    topic,
    record =>
      if (record.topic === topic) keyDeserializer.deserialize(topic, Headers.empty, record.key).map(_ === key)
      else IO.pure(false)
  ).map(_.collect { case (_, _, Some(value)) => value })

  private def selectedHistory[K, V](
      topic: String,
      recordSelector: ProducerRecord[Array[Byte], Array[Byte]] => IO[Boolean]
  )(implicit
      keyDeserializer: KeyDeserializer[IO, K],
      valueDeserializer: ValueDeserializer[IO, V]
  ): IO[List[(Int, K, Option[V])]] = mockProducer.history.asScala.zipWithIndex.toList
    .traverse { case (record, index) =>
      for {
        isSelected <- recordSelector(record)
        key <-
          if (isSelected) keyDeserializer.deserialize(topic, Headers.empty, record.key).map(Option(_))
          else IO(None)
        value <-
          (if (isSelected) {
             if (record.value eq null) IO(Some(None))
             else valueDeserializer.deserialize(topic, Headers.empty, record.value).map(v => Some(Some(v)))
           } else IO(None)): IO[Option[Option[V]]]
      } yield key.zip(value).map { case (k, v) => (index, k, v) }
    }
    .map(_.flatten)

  implicit lazy val mkProducer: MkProducer[IO] = new MkProducer[IO] {
    def apply[G[_]](settings: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] = IO(mockProducer)
  }
}
