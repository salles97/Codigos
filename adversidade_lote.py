
# A função recebe 3 parametros, sendo o terceiro utilizado somente para casos especificos
# Cur: cursor aberto
# name: nome da rotulagem do lote contido no schema public do banco
# adv: Tipo da adversidade, quando identificada no script e nao no rotulo
def adversidade_lote(cur, name, adv):

    if name.startswith('Property'):
        tipoAdv = 'I'
    elif name.split('-')[3] == 'R':
        tipoAdv = 'R'
    elif name.split('-')[3] == 'D':
        tipoAdv = 'D'
    elif adv == 'C':
        tipoAdv = 'C'
    elif adv == 'End':
        tipoAdv = 'End'
    else:
        print('Adversidade não identificada, rótulo: %s', name)
        return

    cur.execute("SELECT * FROM public.lote WHERE name = %s", name)
    lote = cur.fetchone()

    cur.execute(
        'SELECT geom FROM public.area_coberta WHERE ST_Contains(%s, geom)', lote['geom'])
    coberturas = cur.fetchall()

    cur.execute(
        'SELECT geom FROM public.benfeitoria WHERE ST_Contains(%s, geom)', lote['geom'])
    benfeitorias = cur.fetchone()

    cur.execute(
        'SELECT geom FROM public.testada WHERE ST_Contains(ST_Buffer(%s, 0.5), geom)', lote['geom'])
    testadas = cur.fetchone()

    cur.execute('INSERT INTO to_review.lote (name, geom, tipo) VALUES (%s, %s, %s) returning id',
                lote['name'], lote['geom'], tipoAdv)
    idLoteReview = cur.fetchone()

    if coberturas is not None:
        for cobertura in coberturas:
            cur.execute('INSERT INTO to_review.area_coberta (geom, loteReview_id) VALUES (%s, %s)',
                        cobertura['geom'], idLoteReview)

    if benfeitorias is not None:
        for benfeitoria in benfeitorias:
            cur.execute('INSERT INTO to_review.benfeitoria (geom, loteReview_id) VALUES (%s, %s)',
                        benfeitoria['geom'], idLoteReview)

    if testadas is not None:
        for testada in testadas:
            cur.execute('INSERT INTO to_review.testada (geom, loteReview_id) VALUES (%s, %s)',
                        testada['geom'], idLoteReview)
    else:
        print('Lote adversidade também sem testada!!')
