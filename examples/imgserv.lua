--
-- Usage:
--
--   lua imgserv.lua put <pool> <object-id> <filename>
--   lua imgserv.lua put_smart <pool> <object-id> <filename>
--   lua imgserv.lua get <pool> <object-id> <spec-string> <filename>
--   lua imgserv.lua thumb <pool> <object-id> <spec-string>
--
local rados = require "rados"
local clslua = require "clslua"

-- command line arguments
command = arg[1]
pool = arg[2]
object = arg[3]

-- Connect to the RADOS cluster
cluster = rados.create()
cluster:conf_read_file()
cluster:connect()
ioctx = cluster:open_ioctx(pool)

--
--
--
function do_put(object, filename)
  assert(type(object) == 'string')
  assert(type(filename) == 'string')

  -- write image from file into object
  local file = io.open(filename, "rb")
  local img = file:read("*all")
  file:close()
  local size, offset = #img, 0
  ioctx:write(object, img, size, offset)

  -- save the size/offset of the image
  local loc_spec = size .. "@" .. offset
  ioctx:omapset(object, {
    original = loc_spec,
  })
end

--
--
--
function do_put_smart(object, filename)
  assert(type(object) == 'string')
  assert(type(filename) == 'string')

  -- script that will run remotely on the storage
  -- device. it locally performs the image blob
  -- write, and the index update with one round-trip.
  local script = [[
  function put(img)
    -- write the input blob
    local size, offset = #img, 0
    cls.write(offset, size, img)

    -- update the leveldb index
    local loc_spec_bl = bufferlist.new()
    local loc_spec = size .. "@" .. offset
    loc_spec_bl:append(spec)
    cls.map_set_val("original", loc_spec_bl)
  end
  cls.register(store)
  ]]

  -- read the input image from the file
  local file = io.open(filename, "rb")
  local img = file:read("*all")
  file:close()

  -- remotely execute script with image as input
  clslua.exec(ioctx, object, script, "put", img)
end

--
-- Save image from `object` into `filename`.
--
function do_get(object, filename, spec)
  assert(type(object) == 'string')
  assert(type(filename) == 'string')

  local script = [[
  function get(input, output)
    -- lookup the location of the image given the spec
    local loc_spec_bl = cls.map_get_val(input:str())
    local size, offset = string.match(loc_spec_bl:str(), "(%d+)@(%d+)")
    cls.log(0, input:str(), size, offset)

    -- read and return the image blob from the object
    out_bl = cls.read(offset, size)
    output:append(out_bl:str())
  end
  cls.register(get)
  ]]

  -- execute script remotely
  ret, img = clslua.exec(ioctx, object, script, "get", spec)

  -- write image to output file
  local file = io.open(filename, "wb")
  file:write(img)
  file:close()
end

function do_thumb(object, spec_string)
  assert(type(object) == 'string')
  assert(type(spec_string) == 'string')

  local script = [[
  local magick = require "magick"

  function get_orig_img()
    -- lookup the location of the original image
    local loc_spec_bl = cls.map_get_val("original")
    local size, offset = string.match(loc_spec_bl:str(), "(%d+)@(%d+)")

    -- read image into memory
    return cls.read(offset, size)
  end

  function thumb(input, output)
    -- apply thumbnail spec to original image
    local spec_string = input:str()
    local blob = get_orig_img()
    local img = assert(magick.load_image_from_blob(blob:str()))
    img = magick.thumb(img, spec_string)

    -- append thumbnail to object
    local obj_size = cls.stat()
    local img_bl = bufferlist.new()
    img_bl:append(img)
    cls.write(obj_size, #img_bl, img_bl)

    -- save location in leveldb
    local loc_spec = #img_bl .. "@" .. obj_size
    local loc_spec_bl = bufferlist.new()
    loc_spec_bl:append(loc_spec)
    cls.map_set_val(spec_string, loc_spec_bl)
  end

  cls.register(thumb)
  ]]
  
  clslua.exec(ioctx, object, script, "thumb", spec_string)
end

--
-- Run action
--
actions = {
  ["put"] = function () do_put(object, arg[4]) end,
  ["put_smart"] = function () do_put_smart(object, arg[4]) end,
  ["get"] = function () do_get(object, arg[4], arg[5]) end,
  ["thumb"] = function () do_thumb(object, arg[4]) end,
}

actions[command]()
