local Error = {}

function Error:new(code, message)
	local instance =
	{
		code = code or 0,
		message = message or "Unknown error"
	}
	setmetatable(instance, self)
	self.__index = self
	return instance
end

function Error:throw()
	error(self.message)
end

return Error