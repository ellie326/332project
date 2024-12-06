package com.orange.bytestring_ordering

import com.google.protobuf.ByteString

import scala.math.Ordering.comparatorToOrdering

object ByteStringOrdering {
  implicit val ordering: Ordering[ByteString] = comparatorToOrdering(ByteString.unsignedLexicographicalComparator)
}
