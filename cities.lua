local set = {}

local file = io.open("hash.txt", "a")

for city in io.lines("cities.txt") do
	if not set[city] then
		set[city] = true

		if file ~= nil then
			file:write(city .. "\n")
		end
	end
end
