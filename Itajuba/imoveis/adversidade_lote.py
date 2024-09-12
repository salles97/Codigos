import psycopg2

def adversidade_lote(cur, lote, adv, arquivo_log):
    # Certifique-se de acessar o valor da coluna do lote
    name = lote['name']  # Usa um valor padrão se 'name' não estiver presente

    # Abrir o arquivo de relatório usando um context manager
    # arquivo_path = f'relatorio_{setor_carga}_adversidades.txt'
    # with open(arquivo_path, 'a') as arquivo:
        # Determinar o tipo de adversidade
    tipoAdv = {
        'C': 'C',
        'End': 'End',
        'Vago': 'Vago',
        'R': 'R',
        'I': 'I',
        'Outra': 'Outra'
    }.get(adv, None)

    if tipoAdv is None:
        arquivo_log.write(f'Adversidade não identificada, rótulo: {name}\n')
        return

    arquivo_log.write(f'lote: {name} adicionado com adversidade do tipo {tipoAdv}\n')

    try:
        # Executar consulta para obter o lote
        cur.execute("SELECT * FROM public.lote WHERE name = %s", (name,))
        lote = cur.fetchone()
        
        if lote is None:
            arquivo_log.write(f'Lote não encontrado na base de dados: {name}\n')
            return
        
        
        # Executar consultas para obter dados relacionados
        cur.execute('SELECT geom FROM public.area_coberta WHERE ST_Contains(%s, geom)', (lote['geom'],))
        coberturas = cur.fetchall()

        cur.execute('SELECT geom FROM public.benfeitoria WHERE ST_Contains(%s, geom)', (lote['geom'],))
        benfeitorias = cur.fetchall()

        cur.execute('SELECT geom FROM public.testada WHERE ST_Contains(ST_Buffer(%s, 0.5), geom)', (lote['geom'],))
        testadas = cur.fetchall()

        # Inserir lote na tabela to_review
        try:
            cur.execute('INSERT INTO to_review.lote (name, geom, tipo) VALUES (%s, %s, %s) RETURNING id',
                        (lote['name'], lote['geom'], tipoAdv))
            idLoteReview = cur.fetchone()[0]
        except psycopg2.Error as e:
            arquivo_log.write(f'Erro ao inserir lote: {e}\n')
            # cur.rollback()
            return  # Continua para o próximo lote, não encerra a função

        # Inserir coberturas na tabela to_review
        if coberturas:
            for cobertura in coberturas:
                try:
                    cur.execute('INSERT INTO to_review.area_coberta (geom, tipo, lote_id) VALUES (%s, %s, %s)',
                                (cobertura['geom'], 'C', idLoteReview))
                except psycopg2.Error as e:
                    arquivo_log.write(f'Erro ao inserir cobertura : {cobertura["geom"]}. Erro: {e}\n')
                    # cur.rollback()

        # Inserir benfeitorias na tabela to_review
        if benfeitorias:
            for benfeitoria in benfeitorias:
                try:
                    cur.execute('INSERT INTO to_review.area_coberta (geom, tipo, lote_id) VALUES (%s, %s, %s)',
                                (benfeitoria['geom'], 'B', idLoteReview))
                except psycopg2.Error as e:
                    arquivo_log.write(f'Erro ao inserir benfeitoria : {benfeitoria["geom"]}. Erro: {e}\n')
                    # cur.rollback()

        # Inserir testadas na tabela to_review
        if testadas:
            for testada in testadas:
                try:
                    cur.execute('INSERT INTO to_review.testada (geom, lote_id, length) VALUES (ST_Multi(%s), %s, st_length(%s))', 
                                (testada['geom'], idLoteReview, testada['geom']))
                except psycopg2.Error as e:
                    arquivo_log.write(f'Erro ao inserir testada com geom duplicado: {testada["geom"]}. Erro: {e}\n')
                    # cur.rollback()

    except psycopg2.Error as e:
        arquivo_log.write(f'Erro ao processar lote {name}. Erro: {e}\n')
        # cur.rollback()  # Garante que a transação seja revertida em caso de erro
