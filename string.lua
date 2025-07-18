
local String = {}

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
	for part in string.gmatch(input, "[^" .. delimiter .. "]+") do
		table.insert(retval, part)
	end
	return retval
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

function String.hasPrefix(input, suffix)
	return string.sub(input, #suffix) == suffix
end

function String.hasSuffix(input, suffix)
	return string.sub(input, -#suffix) == suffix
end

return String