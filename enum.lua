local Enum = {}

function Enum:new(...)
	local retval = {}
	for _, v in ipairs({...}) do
		retval[v] = v
	end
	return retval
end

return Enum