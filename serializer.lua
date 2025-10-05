local string = require "string"

local Serializer = {}

function Serializer.Encode(input)
	if type(input) ~= "table" then
		return nil
	end
	local list = {}
	local stack = {}
	local set = {}
	local path = {}

	-- Начинаем с корневой таблицы
	table.insert(stack, {t = input, key = nil})
	set[input] = true

	while #stack > 0 do
		local current = stack[#stack]
		local t = current.t
		local key = current.key
		local next_key, key_value = next(t, key)
		if next_key == nil then
			table.remove(stack)
			if #path > 0 then
				table.remove(path) -- Удаляем последний элемент из path
			end
		else
			current.key = next_key
			local key_name = tostring(next_key)
			if type(key_value) == "table" then
				table.insert(path, key_name)
				-- Проверяем, не посещали ли уже эту таблицу
				if not set[key_value] then
					set[key_value] = true
					-- Кладём вложенную таблицу в стек
					table.insert(stack, {t = key_value, key = nil})
				end
			else
				if #path > 0 then
					table.insert(list, table.concat(path, ":")..":"..key_name..":"..tostring(key_value))
				else
					table.insert(list, key_name..":"..tostring(key_value))
				end
			end
		end
	end
	return table.concat(list, "\n")
end

return Serializer