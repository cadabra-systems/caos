local ngx = require "ngx"
local resty_http = require "resty.http"

--[[
	@brief resty.http с выходом наружу по настройке узла и журналом исходящих запросов
	@example local http = require "caos.http" - вместо require "resty.http", остальное как было
	@example CUBE_EGRESS="*.paykeeper.ru=http://prx22.cadabra.cloud:3128;api.stripe.com=http://out:3128"
	@note Связность - свойство места, а не того, кто и куда обращается: с одного узла PayKeeper
	отдаёт тело ответа, а с другого нет. Поэтому прокси назначает узел своим окружением, а клиент
	его не выбирает: connect подставляет его по имени хоста сам, и request_uri, зовущий connect
	изнутри, получает его без единой правки вызывающего кода.
	@note Шаблон - точное имя хоста, *.суффикс или * для всего прочего, записи через точку с запятой.
	Точное имя сильнее шаблона, из шаблонов выигрывает самый длинный. Хост без совпадения ходит
	напрямую, и это же значит пустая или отсутствующая настройка. Переменная должна быть объявлена в
	nginx.conf строкой env CUBE_EGRESS - иначе воркеры её не увидят.
	@note Прокси, названный вызывающим - proxy_opts в connect либо set_proxy_options, - сильнее
	настройки узла: вызывающий знает больше узла.
	@note Один прокси на обе схемы. resty.http берёт для https только https_proxy и молча отбрасывает
	http_proxy - так прокси из DSN базы ?http_proxy=... выглядел настроенным и не действовал.
--]]
local HTTP = setmetatable({}, {__index = resty_http})
local MetaTable = {__index = HTTP}

--[[
	@note Ответ request_uri дольше этого пишется предупреждением даже при успехе
--]]
HTTP.SlowSeconds = 5

local function is_empty(value)
	return value == nil or value == ""
end

--[[
	@note Разбирается один раз на воркер: окружение воркера не меняется. Запись с ошибкой
	пропускается с предупреждением: опечатка в одной записи не должна отрезать узел от остальных.
--]]
local egress = nil

local function egress_setting()
	if egress then
		return egress
	end

	egress = {exact = {}, suffix = {}, default = nil}
	for record in tostring(os.getenv("CUBE_EGRESS") or ""):gmatch("[^;]+") do
		local pattern, proxy = record:match("^%s*([^=%s]+)%s*=%s*(%S+)%s*$")
		pattern = pattern and pattern:lower()
		if not pattern or not proxy:match("^https?://[^/%s]+/?$") then
			ngx.log(ngx.WARN, "CUBE_EGRESS record is skipped, expected host=http://proxy:port: ", record)
		elseif pattern == "*" then
			egress.default = proxy
		elseif pattern:sub(1, 2) == "*." then
			egress.suffix[#egress.suffix + 1] = {suffix = pattern:sub(2), proxy = proxy}
		else
			egress.exact[pattern] = proxy
		end
	end
	table.sort(egress.suffix, function(left, right) return #left.suffix > #right.suffix end)

	return egress
end

--[[
	@brief Прокси для хоста по настройке узла
	@return адрес прокси либо nil - напрямую
--]]
function HTTP.Proxy(host)
	host = tostring(host or ""):lower()
	local setting = egress_setting()
	if setting.exact[host] then
		return setting.exact[host]
	end
	for _, entry in ipairs(setting.suffix) do
		if #host > #entry.suffix and host:sub(-#entry.suffix) == entry.suffix then
			return entry.proxy
		end
	end
	return setting.default
end

--[[
	@brief Адрес прокси для журнала - без учётных данных: http://user:password@host бывает и так
--]]
function HTTP.SafeAddress(proxy)
	return (tostring(proxy or ""):gsub("^(https?://)[^@/]*@", "%1"))
end

function HTTP.new()
	local client, failure = resty_http.new()
	if not client then
		return nil, failure
	end
	return setmetatable(client, MetaTable)
end

--[[
	@brief connect из resty.http, но с прокси по настройке узла
	@note Названный вызывающим прокси не трогается. Прежняя сигнатура connect(host, port) - голый TCP
	без прокси и у resty.http, - уходит как есть.
	@note Один адрес на обе схемы: названный только под одну схему распространяется на другую.
--]]
function HTTP:connect(options, ...)
	if type(options) ~= "table" then
		return resty_http.connect(self, options, ...)
	end

	local named = options.proxy_opts or self.proxy_opts
	local proxy = named and (not is_empty(named.https_proxy) and named.https_proxy or named.http_proxy) or nil
	if is_empty(proxy) and not (named and named.no_proxy) then
		proxy = HTTP.Proxy(options.host)
	end

	options.proxy_opts = nil
	if not is_empty(proxy) then
		options.proxy_opts = {http_proxy = proxy, https_proxy = proxy, no_proxy = named and named.no_proxy or nil}
	end
	self.egress_proxy = options.proxy_opts and proxy or nil

	return resty_http.connect(self, options)
end

local function elapsed(since)
	ngx.update_time()
	return ngx.now() - since
end

local function seconds(value)
	return ("%.3f"):format(value)
end

--[[
	@note Заголовки рамки ответа: по ним клиент решает, где кончается тело. Секретов в них не бывает.
--]]
local function framing(response)
	local header = response.headers or {}
	return ("reason %s, length %s, transfer %s, connection %s, type %s, encoding %s"):format
	(
		tostring(response.reason or "-"),
		tostring(header["Content-Length"] or "-"),
		tostring(header["Transfer-Encoding"] or "-"),
		tostring(header["Connection"] or "-"),
		tostring(header["Content-Type"] or "-"),
		tostring(header["Content-Encoding"] or "-")
	)
end

--[[
	@brief request_uri из resty.http, но по фазам и с журналом: соединение, заголовки, тело
	@param params как у resty.http; label - необязательное имя запроса для журнала
	@note Журнал нужен затем, что ошибка request_uri одна на все случаи: "timeout" не говорит,
	повисло ли соединение, рукопожатие или ожидание тела, и свежим было соединение или из пула. Так
	и нашлось, что cdn21 получает от PayKeeper заголовки и первые 16 КБ ответа, а остального нет.
	@note Адрес в журнал не пишется никогда - только метод, хост и label: у входящих вебхуков
	Bitrix24 ключ лежит в пути.
	@note Удачные ответы - INFO, медленные и отказы - WARN.
--]]
function HTTP:request_uri(uri, params)
	params = params or {}

	local parsed, failure = self:parse_uri(uri, false)
	if not parsed then
		return nil, failure
	end

	local scheme, host, port, path, query = unpack(parsed)
	local label = (params.method or "GET")..(is_empty(params.label) and "" or (" "..tostring(params.label)))
	local size = type(params.body) == "string" and #params.body or 0

	ngx.update_time()
	local start = ngx.now()

	local connected
	connected, failure = self:connect
	(
		{
			scheme = scheme,
			host = host,
			port = port,
			ssl_server_name = params.ssl_server_name or host,
			ssl_verify = params.ssl_verify ~= false,
			ssl_reused_session = params.ssl_reused_session,
			ssl_send_status_req = params.ssl_send_status_req,
			ssl_client_cert = params.ssl_client_cert,
			ssl_client_priv_key = params.ssl_client_priv_key,
			pool = params.pool,
			pool_size = params.pool_size,
			backlog = params.backlog,
			proxy_opts = params.proxy_opts
		}
	)
	local route = host..(self.egress_proxy and (" via "..HTTP.SafeAddress(self.egress_proxy)) or "")
	if not connected then
		ngx.log(ngx.WARN, "HTTP ", label, " to ", route, ": connect failed after ", seconds(elapsed(start)), "s: ", tostring(failure))
		return nil, failure
	end
	local connect_time = seconds(elapsed(start))
	-- @note Ноль - соединение свежее, больше нуля - взято из пула воркера
	local reused = self:get_reused_times() or 0

	local response
	response, failure = self:request
	(
		{
			version = params.version,
			method = params.method or "GET",
			path = params.path or path,
			query = params.query or query,
			headers = params.headers,
			body = params.body
		}
	)
	if not response then
		ngx.log(ngx.WARN, "HTTP ", label, " to ", route, ": no response after ", seconds(elapsed(start)), "s (connect ", connect_time, "s, reused ", reused, ", sent ", size, " bytes): ", tostring(failure))
		self:close()
		return nil, failure
	end
	local header_time = seconds(elapsed(start))

	local body
	body, failure = response:read_body()
	if not body then
		ngx.log(ngx.WARN, "HTTP ", label, " to ", route, ": body is not read after ", seconds(elapsed(start)), "s (status ", response.status, ", headers ", header_time, "s, reused ", reused, ", sent ", size, " bytes; ", framing(response), "): ", tostring(failure))
		self:close()
		return nil, failure
	end
	response.body = body

	local total = elapsed(start)
	ngx.log(total > HTTP.SlowSeconds and ngx.WARN or ngx.INFO, "HTTP ", label, " to ", route, ": ", response.status, " in ", seconds(total), "s (connect ", connect_time, "s, headers ", header_time, "s, reused ", reused, ", sent ", size, " bytes, received ", #body, " bytes; ", framing(response), ")")

	if params.keepalive == false then
		local closed
		closed, failure = self:close()
		if not closed then
			ngx.log(ngx.ERR, failure)
		end
	else
		local kept
		kept, failure = self:set_keepalive(params.keepalive_timeout, params.keepalive_pool)
		if not kept then
			ngx.log(ngx.ERR, failure)
		end
	end

	return response, nil
end

return HTTP
