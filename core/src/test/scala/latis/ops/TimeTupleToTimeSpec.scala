package latis.ops

import latis.data._
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter
import latis.util.StreamUtils

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class TimeTupleToTimeSpec extends FlatSpec {
  
  def mockDataset: MemoizedDataset = {
    val metadata: Metadata = Metadata("TestTimeTupleFlux" + ("title" -> "Test Time Tuple Flux"))
    val model: DataType = Function(
      Tuple(Metadata("time"),
        Scalar(Metadata("year") + ("type" -> "int") + ("units" -> "yyyy")),
        Scalar(Metadata("month") + ("type" -> "int") + ("units" -> "MM")),
        Scalar(Metadata("day") + ("type" -> "int") + ("units" -> "dd")),
      ),
      Scalar(Metadata("flux") + ("type" -> "int"))
    )
    val data: MemoizedFunction = SeqFunction(
      Seq(
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(1)), RangeData(10)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(2)), RangeData(20)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(3)), RangeData(30)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(4)), RangeData(40)),
        Sample(DomainData(Data.IntValue(1945), Data.IntValue(1), Data.IntValue(5)), RangeData(50))
      )
    )
    new MemoizedDataset(metadata, model, data)
  }

  "The mock dataset's time tuple" should "be converted to a time scalar" in {
    val ds = mockDataset

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Number(y), Number(m), Number(d)), RangeData(Number(f))) =>
        y should be (1945)
        m should be (1)
        d should be (1)
        f should be (10)
    }
    
    val ds2 = ds.withOperation(TimeTupleToTime())

    StreamUtils.unsafeHead(ds2.samples) match {
      case Sample(DomainData(Text(time)), RangeData(Number(f))) =>
        time should be ("1945 1 1")
        f should be (10)
    }

    //TextWriter().write(ds)
  }
}
