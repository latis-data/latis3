package latis.server

import java.net.URL

import com.comcast.ip4s.port
import fs2.io.file.Path
import pureconfig.ConfigSource

class ConfSuite extends munit.FunSuite {

  test("read CatalogConf") {
    val dir = "/path/to/dir"
    val validate = true

    val conf = ConfigSource.string(s"""
    {
      dir = $dir
      validate = $validate
    }
    """).load[CatalogConf]

    val expected = CatalogConf(Path(dir), validate)

    assertEquals(conf, Right(expected))
  }

  test("read ServerConf") {
    val port = port"1234"
    val prefix = "dap2"

    val conf = ConfigSource.string(s"""
    {
      port = $port
      prefix = $prefix
    }
    """).load[ServerConf]

    val expected = ServerConf(port, prefix)

    assertEquals(conf, Right(expected))
  }

  test("read ServiceConf") {
    val conf = ConfigSource.string("""
    {
      services = [
        { type = class, prefix = "prefix", class = "class" },
        { type = class, prefix = "prefix", class = "class" }
      ]
    }
    """).load[ServiceConf]

    assertEquals(conf.map(_.services.length), Right(2))
  }

  test("fail if type is unknown") {
    val conf = ConfigSource.string("""
    {
      services = [
        { type = fake, prefix = "prefix", class = "class" }
      ]
    }
    """).load[ServiceConf]

    assert(conf.isLeft)
  }

  test("read JarServiceSpec") {
    val path = URL("file://path/to/jar")
    val prefix = "service"
    val clss = "ServiceClass"

    val conf = ConfigSource.string(s"""
    {
      type = jar
      path = "$path"
      prefix = $prefix
      class = $clss
    }
    """).load[ServiceSpec]

    val expected = JarServiceSpec(path, prefix, clss)

    assertEquals(conf, Right(expected))
  }

  test("read ClassPathServiceSpec") {
    val prefix = "service"
    val clss = "ServiceClass"

    val conf = ConfigSource.string(s"""
    {
      type = class
      prefix = $prefix
      class = $clss
    }
    """).load[ServiceSpec]

    val expected = ClassPathServiceSpec(prefix, clss)

    assertEquals(conf, Right(expected))
  }
}
