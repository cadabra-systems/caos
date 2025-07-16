local ngx = require "ngx"
local cjson = require "cjson"
local http = require "resty.http"
local string = require "caos.string"
local url = require "caos.url"

local OracleDatabase = {}

function OracleDatabase:new(connection_string)
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

function OracleDatabase:connect()
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
			ssl_verify = false
		}
	)
	if not retcode or error then
		ngx.log(ngx.ERR, "OracleDatabase connect to ", self.hostname..":"..tostring(self.hostport), " error: ", error or "Unknown")
		self.session = nil
		return false
	end
	self.session = session
	return true
end

function OracleDatabase:disconnect()
	if not self.client then
		return false
	end
	return self.client:close()
end

function OracleDatabase:query(q, ...)
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
				path = "/ords/"..self.dbname.."/_/sql",
				headers =
				{
					['Host'] = self.hostname,
					['Content-Type'] = "application/sql",
					['Content-Length'] = q:len(),
					['Connection'] = "Keep-Alive",
					['User-Agent'] = "Caos/0.1",
					['Authorization'] = "Basic ".. ngx.encode_base64(self.username..":"..self.password)
				},
				query =
				{
				},
				body = q
			}
		)
		if call then
			response = call
			break
		elseif i > 9 then
			ngx.log(ngx.ERR, "OracleDatabase request error: ", error or "Unknown")
			return {error = 2, selected_rows = 0, read_rows = 0, affected_rows = 0}
		elseif not self:connect() then
			ngx.log(ngx.INFO, "OracleDatabase reconnect round: ", i)
		else
			ngx.log(ngx.INFO, "OracleDatabase reconnected at round: ", i)
		end
	end
	ngx.log(ngx.DEBUG, "OracleDatabase query q: ", q:gsub("[\n\r]", " "), "; status: ", response.status, "; id: ", response.headers['X-OracleDatabase-Query-Id'] or "none")
	if response.status ~= 200 then
		ngx.log(ngx.ERR, "OracleDatabase query error: ", response:read_body())
		return {error = -1, selected_rows = 0, read_rows = 0, affected_rows = 0, rowset = nil}
	elseif response.headers['Content-Type'] ~= "application/json" then
		ngx.log(ngx.ERR, "OracleDatabase query response unexpected content type: ", response.headers['Content-Type'])
		return {error = -1, selected_rows = 0, read_rows = 0, affected_rows = 0, rowset = nil}
	end

	local retval =
	{
		id = "0",
		error = 0,
		selected_rows = 0,
		read_rows = 0,
		affected_rows = 0,
		rowset = nil
	}

	local response_body = ""
	local response_body_reader = response.body_reader
	repeat
		local buffer, body_reader_error = response_body_reader(8192)
		if body_reader_error then
			ngx.log(ngx.ERR, "OracleDatabase response reader error: ", body_reader_error)
			retval.error = 3
			return retval
		elseif buffer then
			response_body = response_body .. buffer
		end
	until not buffer

	local retcode, error = self.client:set_keepalive()
	if not retcode or retcode ~= 1 then
		ngx.log(ngx.INFO, "OracleDatabase keepalive error: ", error or "Unknown")
		self.client = nil
		self.session = nil
	end

	if not string.isEmpty(response_body) then
		local response = cjson.decode(response_body)
		if not response or #response['items'] < 1 then
			ngx.log(ngx.ERR, "OracleDatabase response format error: ", response_body)
			retval.error = 4
			return retval
		end
		response = response['items'][1]
		if response['errorCode'] then
			--errorCode: 911
            --errorLine: 2
            --errorColumn: 3
            --errorMessage: ORA-00911: invalid character after SELECT\n\nhttps://docs.oracle.com/error-help/db/ora-00911/
            --errorDetails: 00911. 00000 -  \"%s: invalid character after %s\" *Cause:    An invalid character has been encountered in the SQL statement. Action: Remove the invalid character. If it is part of an identifier, enclose the identifier in double quotation marks. Params: 1) character_value 2) token_value: The token after which the invalid character causing the error occurs.
			retval.error = tonumber(response['errorCode'])
			return retval
		elseif response['resultSet'] then
--[[
			"metadata":
			[
				{
					"columnName": "ID",
					"jsonColumnName": "id",
					"columnTypeName": "RAW",
					"columnClassName": "[B",
					"precision": 16,
					"scale": 0,
					"isNullable": 0
				}
			]
			,
			"items":
            [
                {
					"id": "lSESTLB5TVSVHpehEZHJ7g==",
					"content_type": "video/x-cube",
					"channel": "DrOblozhko",
					"collection": "Live",
					"item": "14052024",
					"revision": "2024-01-01T00:00:00Z",
					"meta": null,
					"license": 2,
					"available": true,
					"loggable": true,
					"trackable": false
				}
			]
			,
			"hasMore": false,
            "limit": 10000,
            "offset": 0,
            "count": 149
--]]
			retval.rowset = response['resultSet']['items'] or nil
			if retval.rowset then
				retval.selected_rows = response['resultSet']['count'] or 0
			end
		end
	end

	return retval
end

function OracleDatabase:insert(table_name, fields)
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

	return self:query("INSERT INTO "..table_name.."("..table.concat(name_list, ", ")..") VALUES("..table.concat(value_list, ", ")..")")
end

function OracleDatabase:call(name, body)
	if not name or not self.client then
		return {error = 1, body = {}}
	end
	body = cjson.encode(body or {})

	local response = {}
	for i = 1, 10, 1 do
		local call, error = self.client:request
		(
			{
				method = "POST",
				path = "/ords/"..self.dbname.."/"..name,
				headers =
				{
					['Host'] = self.hostname,
					['Content-Type'] = "application/json",
					['Content-Length'] = body:len(),
					['Connection'] = "Keep-Alive",
					['User-Agent'] = "Caos/0.1",
					['Authorization'] = "Basic ".. ngx.encode_base64(self.username..":"..self.password)
				},
				query =
				{
				},
				body = body
			}
		)
		if call then
			response = call
			break
		elseif i > 9 then
			ngx.log(ngx.ERR, "OracleDatabase request error: ", error or "Unknown")
			return nil
		elseif not self:connect() then
			ngx.log(ngx.INFO, "OracleDatabase reconnect round: ", i)
		else
			ngx.log(ngx.INFO, "OracleDatabase reconnected at round: ", i)
		end
	end
	if response.status ~= 200 then
		ngx.log(ngx.ERR, "OracleDatabase request error: ", response:read_body())
		return {error = response.status, body = {}}
	elseif response.headers['Content-Type'] ~= "application/json" then
		return {error = -1, body = {}}
	end

	local retval = {error = 0, body = {}}

	local response_body = ""
	local response_body_reader = response.body_reader
	repeat
		local buffer, body_reader_error = response_body_reader(8192)
		if body_reader_error then
			ngx.log(ngx.ERR, "OracleDatabase response reader error: ", body_reader_error)
			retval.error = 3
			return retval
		elseif buffer then
			response_body = response_body .. buffer
		end
	until not buffer

	local retcode, error = self.client:set_keepalive()
	if not retcode or retcode ~= 1 then
		ngx.log(ngx.INFO, "OracleDatabase keepalive error: ", error or "Unknown")
		self.client = nil
		self.session = nil
	end
	if not string.isEmpty(response_body) then
		retval.body = cjson.decode(response_body)
		if retval.body then
			if retval.body.status then
				if retval.body.status ~= 200 then
					retval.error = retval.body.status
				end
				if retval.body.result then
					retval.body = cjson.decode(retval.body.result)
				end
			end
		end
	end
	return retval
end

return OracleDatabase