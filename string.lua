
local String = {}

function String.subString(input, offset, count)
	return string.sub(input, offset, count)
end

function String.count(input, pattern)
	local retval = 0
	for word in string.gmatch(input, pattern) do
		retval = retval + 1
	end
	return retval
end

function String.trim(input)
	return input:match("^%s*(.-)%s*$")
end

function String.split(input, delimiter)
	local retval = {}
	local offset = 1
	local length = #delimiter
	while true do
		local i = string.find(input, delimiter, offset, true)
		if not i then
			table.insert(retval, input:sub(offset))
			break
		end
		table.insert(retval, input:sub(offset, i - 1))
		offset = i + length
	end
	return retval
end

function String.findFirst(input, needle)
	for i = 1, #input - #needle + 1 do
		if string.sub(input, i, i + #needle - 1) == needle then
			return i
		end
	end
	return nil
end

function String.findLast(input, needle)
	for i = #input - #needle + 1, 1, -1 do
		if string.sub(input, i, i + #needle - 1) == needle then
			return i
		end
	end
	return nil
end

function String.isEmpty(input)
	return input == nil or input == ""
end

function String.hasPrefix(input, prefix)
	if String.isEmpty(input) or String.isEmpty(prefix) then
		return false
	end
	return string.sub(input, 1, #prefix) == prefix
end

function String.hasSuffix(input, suffix)
	if String.isEmpty(input) or String.isEmpty(suffix) then
		return false
	end
	return string.sub(input, -#suffix) == suffix
end

return String