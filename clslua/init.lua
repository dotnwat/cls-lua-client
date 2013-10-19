local mp = require 'MessagePack'

local function argerror(caller, narg, extramsg)
  error("bad argument #" .. tostring(narg) .. " to "
    .. caller .. " (" .. extramsg .. ")")
end

local function typeerror (caller, narg, arg, tname)
  argerror(caller, narg, tname .. " expected, got " .. type(arg))
end

local function checktype (caller, narg, arg, tname)
  if type(arg) ~= tname then
    typeerror(caller, narg, arg, tname)
  end
end

local exec
exec = function(ioctx, oid, script, func, input)
  checktype('exec', 1, ioctx, 'userdata')
  checktype('exec', 2, oid, 'string')
  checktype('exec', 3, script, 'string')
  checktype('exec', 4, func, 'string')
  checktype('exec', 5, input, 'string')

  -- build the cls_lua command
  cmd = {[1] = script, [2] = func, [3] = input}
  packed_input = mp.pack(cmd)

  -- run script on osd
  ret, outdata = ioctx:exec(oid, "lua", "eval", packed_input, #packed_input)
  return ret, outdata
end

return {
  exec = exec,
}
