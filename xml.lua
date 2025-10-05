local string = require "string"

local XML = {}

XML.Indent = "	"

function XML.Escape(input)
	if type(input) ~= "string" then
		return tostring(input)
	end
	return input
	:gsub("&", "&amp;")
	:gsub("<", "&lt;")
	:gsub(">", "&gt;")
	:gsub("\"", "&quot;")
	:gsub("'", "&apos;")
end

function XML.IsArray(input)
	if type(input) ~= "table" then return false end
	local count = 0
	for _ in pairs(input) do
		count = count + 1
	end
	for i = 1, count do
		if input[i] == nil then return false end
	end
	return true
end

function XML.Encode(input, root_name)
	root_name = root_name or "root"

	local retval = {}
	if type(input) == "table" then
		if root_name then
			table.insert(retval, XML.Indent.."<"..root_name..">")
		end

		if XML.IsArray(input) then
			-- Handle array
			for _, v in ipairs(input) do
				if type(v) == "table" then
					-- Use 'item' as default tag for array elements
					table.insert(retval, XML.Encode(v, "item"))
				else
					table.insert(retval, XML.Indent.."  <item>"..XML.Escape(v).."</item>")
				end
			end
		else
			-- Handle object
			for k, v in pairs(input) do
				if type(v) == "table" then
					table.insert(retval, XML.Encode(v, k))
				else
					table.insert(retval, XML.Indent.."  <"..k..">"..XML.Escape(v).."</"..k..">")
				end
			end
		end

		if root_name then
			table.insert(retval, XML.Indent.."</"..root_name..">")
		end
	else
		if root_name then
			table.insert(retval, XML.Indent.."<"..root_name..">"..XML.Escape(input).."</"..root_name..">")
		else
			table.insert(retval, XML.Indent..XML.Escapepe(input))
		end
	end
	return table.concat(retval, "\n")
end

return XML