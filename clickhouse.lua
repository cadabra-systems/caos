local ngx = require "ngx"
local cjson = require "cjson"
local http = require "resty.http"
local string = require "caos.string"
local url = require "caos.url"

local ClickHouse = {}

function ClickHouse:new(connection_string)
	local dsn = url.parse(connection_string or "")
	local instance =
	{
		client = http.new(),
		session = nil,
		certificate = dsn.query.certificate,
		hostname = dsn.host or "localhost",
		hostport = dsn.port or 443,
		dbname = (dsn.path or "/db"):sub(2),
		username = dsn.user or "anonymous",
		password = dsn.password or ""
	}
	setmetatable(instance, self)
	self.__index = self
	return instance
end

function ClickHouse:connect()
	if not self.client or not self.hostname or string.isEmpty(self.hostname) or not self.hostport or self.hostport < 0 then
		return false
	else
		self:disconnect()
	end
	-- @todo timeout?
	local retcode, error, session = self.client:connect
	(
		{
			scheme = "https",
			host = self.hostname,
			port = self.hostport,
			ssl_client_cert = self.certificate,
			ssl_verify = false
		}
	)
	if not retcode or error then
		ngx.log(ngx.ERR, "ClickHouse connect to ", self.hostname..":"..tostring(self.hostport), " error: ", error or "Unknown")
		self.session = nil
		return false
	end
	self.session = session
	return true
end

function ClickHouse:disconnect()
	if not self.client then
		return false
	end
	return self.client:close()
end

function ClickHouse:query(q, ...)
	if not self.client then
		return {error = 1, selected_rows = 0, read_rows = 0, affected_rows = 0, rowset = nil}
	end

	q = string.trim(q)
	for i = 1, select('#', ...) do
		local v = select(i, ...)
		if not v then
			q = q:gsub("%%"..tostring(i), "NULL")
		elseif type(v) == "string" then
			q = q:gsub("%%"..tostring(i), "'"..v.."'")
		else
			q = q:gsub("%%"..tostring(i), tostring(v))
		end
	end

	local response = {}
	for i = 1, 10, 1 do
		local call, error = self.client:request
		(
			{
				method = "POST",
				path = "/",
				headers =
				{
					['Host'] = self.hostname,
					['Content-Type'] = "text/plain",
					['Content-Length'] = q:len(),
					['Connection'] = "Keep-Alive",
					['User-Agent'] = "Caos/0.1",
					['X-ClickHouse-Format'] = "json",
					['X-ClickHouse-User'] = self.username,
					['X-ClickHouse-Key'] = self.password
				},
				query =
				{
					database = self.dbname
				},
				body = q
			}
		)
		if call then
			response = call
			break
		elseif i > 9 then
			ngx.log(ngx.ERR, "ClickHouse request error: ", error or "Unknown")
			return {error = 2, selected_rows = 0, read_rows = 0, affected_rows = 0}
		elseif not self:connect() then
			ngx.log(ngx.INFO, "ClickHouse reconnect round: ", i)
		else
			ngx.log(ngx.INFO, "ClickHouse reconnected at round: ", i)
		end
	end
	ngx.log(ngx.DEBUG, "ClickHouse query q: ", q:gsub("[\n\r]", " "), "; status: ", response.status, "; id: ", response.headers['X-ClickHouse-Query-Id'] or "none")
	if response.status ~= 200 then
		ngx.log(ngx.ERR, "ClickHouse query error: ", response:read_body())
		return {error = -1, selected_rows = 0, read_rows = 0, affected_rows = 0, rowset = nil}
	end

	-- X-ClickHouse-Summary: {"read_rows":"1","read_bytes":"49","written_rows":"1","written_bytes":"143","total_rows_to_read":"1","result_rows":"1","result_bytes":"143","elapsed_ns":"4819095"}
	local summary = cjson.decode(response.headers['X-ClickHouse-Summary'] or {})

	local retval =
	{
		id = response.headers['X-ClickHouse-Query-Id'],
		error = 0,
		selected_rows = 0,
		read_rows = tonumber(summary['read_rows']) or 0,
		affected_rows = tonumber(summary['written_rows']) or 0,
		rowset = nil
	}

	local response_body = ""
	local response_body_reader = response.body_reader
	repeat
		local buffer, body_reader_error = response_body_reader(8192)
		if body_reader_error then
			ngx.log(ngx.ERR, "ClickHouse response reader error: ", body_reader_error)
			retval.error = 3
			return retval
		elseif buffer then
			response_body = response_body .. buffer
		end
	until not buffer

	local retcode, error = self.client:set_keepalive()
	if not retcode or retcode ~= 1 then
		ngx.log(ngx.INFO, "ClickHouse keepalive error: ", error or "Unknown")
		self.client = nil
		self.session = nil
	end

	if not string.isEmpty(response_body) then
		local response = cjson.decode(response_body)
		if not response then
			ngx.log(ngx.ERR, "ClickHouse response format error: ", response_body)
			retval.error = 4
			return retval
		end
		retval.selected_rows = response.rows
		if retval.selected_rows > 0 then
			retval.rowset = response.data
		else
			retval.rowset = nil
		end
	end

	return retval
end

function ClickHouse:insert(table_name, fields)
	if string.isEmpty(table_name) or not fields then
		return {error = 5, selected_rows = 0, read_rows = 0, affected_rows = 0, rowset = nil}
	end

	local name_list, value_list = {}, {}
	for name, value in pairs(fields) do
		while value ~= nil do
			if type(value) == "string" then
				table.insert(value_list, "'"..value.."'")
			elseif type(value) == "date" then
				table.insert(value_list, "'"..tostring(value).."'")
			elseif type(value) == "boolean" then
				table.insert(value_list, tostring(value))
			else
				table.insert(value_list, tostring(value))
			end
			table.insert(name_list, "`".. name .."`")
			break
		end
	end

	if not name_list or not value_list then
		return {error = 5, selected_rows = 0, read_rows = 0, affected_rows = 0, rowset = nil}
	end

	return self:query("INSERT INTO `"..table_name.."`("..table.concat(name_list, ", ")..") VALUES("..table.concat(value_list, ", ")..")")
end

return ClickHouse