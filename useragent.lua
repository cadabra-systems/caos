local ngx = require "ngx"
local string = require "caos.string"

local UserAgent = {}

function UserAgent.Parse(input)
	if string.isEmpty(input) then
		return "crn:cc:agent:anonymous"
	elseif string.hasPrefix(input, "crn:cc:") then
		return input
	end
	input = input:lower()
	-- @example Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15
	local browser, _ = ngx.re.match(input, [[(firefox|fxios|edg|edga|edgios|chrome|crios|safari|opr|opera|version|msie|trident|brave|vivaldi|stagefright|curl)\/([\d.]+)]], "jo")
	if not browser or #browser < 2 then
		return "crn:cc:agent:unknown:"..input
	elseif browser[1] == "version" then
		browser[1] = "safari"
	elseif browser[1] == "edg" then
		browser[1] = "edge"
	elseif browser[1] == "opr" then
		browser[1] = "opera"
	end
	return "crn:cc:agent:"..browser[1]..":"..browser[2] -- @todo ..":"..unique_cookie
end

return UserAgent