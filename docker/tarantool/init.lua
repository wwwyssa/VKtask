box.cfg({
    listen = 3301
})

box.schema.space.create('KV', { if_not_exists = true })
box.space.KV:format({
    { name = 'key', type = 'string' },
    { name = 'value', type = 'varbinary', is_nullable = true }
})
box.space.KV:create_index('primary', {
    parts = { { field = 'key', type = 'string' } },
    if_not_exists = true
})

box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, { if_not_exists = true })

print('Tarantool KV initialized')
