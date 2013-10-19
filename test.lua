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
