using HTTP, Sockets, JSON2

mutable struct Animal
    id::Int
    type::String
    name::String
end
const ANIMALS = Dict{Int, Animal}()
const ANIMAL_ROUTER = HTTP.Router()

function JSONHandler(req::HTTP.Request)
    @info "in JSONHandler"
    # first check if there's any request body
    body = IOBuffer(HTTP.payload(req))
    if eof(body)
        # no request body
        @info "calling HTTP.handle(ANIMAL_ROUTER, req) since NO body"
        response_body = HTTP.handle(ANIMAL_ROUTER, req)
    else
        # there's a body, so pass it on to the handler we dispatch to
        @info "calling HTTP.handle(ANIMAL_ROUTER, req, JSON2.read(body, Animal))"
        response_body = HTTP.handle(ANIMAL_ROUTER, req, JSON2.read(body, Animal))
    end
    return HTTP.Response(200, JSON2.write(response_body))
end

# **simplified** "service" functions
function createAnimal(req::HTTP.Request, animal)
    ANIMALS[animal.id] = animal
    return animal
end

function getAnimal(req::HTTP.Request)
    id = parse(Int, HTTP.URIs.splitpath(req.target)[5]) # /api/zoo/v1/animals/10, get 10
    return ANIMALS[id]
end

function updateAnimal(req::HTTP.Request, animal)
    ANIMALS[animal.id] = animal
    return animal
end

function deleteAnimal(req::HTTP.Request)
    id = parse(Int, HTTP.URIs.splitpath(req.target)[5]) # /api/zoo/v1/animals/10, get 10
    delete!(ANIMALS, id)
    return ""
end
HTTP.@register(ANIMAL_ROUTER, "POST", "/api/zoo/v1/animals", createAnimal)
HTTP.@register(ANIMAL_ROUTER, "GET", "/api/zoo/v1/animals/*", getAnimal)
HTTP.@register(ANIMAL_ROUTER, "PUT", "/api/zoo/v1/animals", updateAnimal)
HTTP.@register(ANIMAL_ROUTER, "DELETE", "/api/zoo/v1/animals/*", deleteAnimal)

function HTTP.handle(::HTTP.Handlers.RequestHandlerFunction{typeof(createAnimal)},
                     req::HTTP.Request, animal::Animal)
    return createAnimal(req, animal)
end
function HTTP.handle(::HTTP.Handlers.RequestHandlerFunction{typeof(updateAnimal)},
                     req::HTTP.Request, animal::Animal)
    return createAnimal(req, animal)
end

@async HTTP.serve(JSONHandler, Sockets.localhost, 8081)

