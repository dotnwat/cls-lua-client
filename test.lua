local rados = require "rados"
local clslua = require "clslua"

describe("exec", function()
  local ioctx

  before_each(function()
    cluster = rados.create()
    cluster:conf_read_file()
    cluster:connect()
    ioctx = cluster:open_ioctx('data')
  end)

  it("throws error with non-string oid", function()
    assert.error(function()
      clslua.exec(ioctx, 5, "", "", "")
    end)
  end)

  it("throws error with non-string script", function()
    assert.error(function()
      clslua.exec(ioctx, "", 5, "", "")
    end)
  end)

  it("throws error with non-string function", function()
    assert.error(function()
      clslua.exec(ioctx, "", "", 5, "")
    end)
  end)

  it("throws error with non-string input", function()
    assert.error(function()
      clslua.exec(ioctx, "", "", "", 5)
    end)
  end)

  it("runs an empty function", function()
    script = "function func() end; cls.register(func)"
    clslua.exec(ioctx, "oid", script, "func", "input")
  end)

  it("runs the say_hello test", function()
    local script = [[
    function say_hello(input, output)
      if #input > 100 then
        return -cls.EINVAL
      end
      output:append("Hello, ")
      if #input == 0 then
        output:append("world")
      else
        output:append(input:str())
      end
      output:append("!")
    end
    cls.register(say_hello)
    ]]

    local ret, outdata = clslua.exec(ioctx, "oid", script, "say_hello", "")
    assert.is_equal(ret, 0)
    assert(outdata == "Hello, world!")

    local ret, outdata = clslua.exec(ioctx, "oid", script, "say_hello", "John")
    assert.is_equal(ret, 0)
    assert(outdata == "Hello, John!")
  end)

  it("runs the echo test", function()
    local script = [[
      function echo(input, output)
        output:append(input:str())
      end
      cls.register(echo)
    ]]
    local input = "what the hell, jerry?"
    ret, outdata = clslua.exec(ioctx, "oid", script, "echo", input)
    assert.is_equal(ret, 0)
    assert.is_equal(outdata, input)
  end)

end)
