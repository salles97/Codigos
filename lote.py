import psycopg2


def criar_lote(cur):
    cur.execute("SELECT * FROM public.lote")
    lotes = cur.fetchall()

    for lote in lotes:
        nome = lote['name']  # assumindo que 'name' é o nome da coluna
        partes = nome.split('-')

        if len(partes) == 4 or partes[0].startswith("Property"):
            adversidade_lote(cur, lote)

        elif len(partes) == 3:
            if partes[2].isdigit():
                try:
                    cur.execute("SELECT * FROM dado_antigo.lote WHERE setor_cod = %s AND quadra_cod = %s AND lote_cod = %s",
                                (partes[0], partes[1], partes[2]))
                    infoLote = cur.fetchone()
                    if infoLote is None:
                        raise Exception(
                            'Lote não encontrado em dado_antigo.lote')

                    reduzido = infoLote['id']

                    hasAddres = consultaCadastraEnd(reduzido, cur)
                    if not hasAddres:
                        raise Exception('Endereço não pode ser cadastrado')

                    geomLote = lote['geom']
                    if geomLote is None:
                        raise Exception(
                            'GEOM do Lote não encontrado em public.lote')

                    cur.execute(
                        "SELECT count(*) FROM dado_antigo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
                    unidades = cur.fetchone()[0]

                    cur.execute("SELECT ac.geom FROM public.lote l, public.area_coberta ac "
                                "WHERE st_contains(l.geom, ac.geom) AND l.name = %s", (nome,))
                    cobertura_geom = cur.fetchone()

                    if unidades == 0 and cobertura_geom is not None:
                        if cadastra_lote(reduzido, True, cur):
                            if cadastraTestada(reduzido, cur):
                                if criarUnidade(reduzido, True, cur):
                                    if CriarEassociarCobertura(reduzido, cur):
                                        print(
                                            'LOTE, TESTADA, UNIDADE E COBERTURAS %s CADASTRADO COM SUCESSO' % reduzido)
                                        continue

                    elif unidades > 0 and cobertura_geom is not None:
                        if cadastra_lote(reduzido, False, cur):
                            if cadastraTestada(reduzido, cur):
                                if criarUnidade(reduzido, False, cur):
                                    if CriarEassociarCobertura(reduzido, cur):
                                        print(
                                            'LOTE, TESTADA, UNIDADE E COBERTURAS %s CADASTRADO COM SUCESSO' % reduzido)
                                        continue

                    elif unidades >= 0 and cobertura_geom is None:
                        if cadastraLoteVago(reduzido, cur) and cadastraTestada(reduzido, cur):
                            print(
                                'LOTE e TESTADAS %s CADASTRADO COM SUCESSO' % reduzido)
                            return True

                except Exception as e:
                    print(e)
                    return False
            else:
                print(nome)
        else:
            print(nome)

    return False


def cadastra_lote(reduzido, vago, cur):
    reduzido_str = str(reduzido)

    cur.execute("SELECT * FROM dado_antigo.lote WHERE id = %s", (reduzido,))
    lote = cur.fetchone()
    if lote is None:
        raise Exception('Lote não encontrado em dado_antigo.lote')

    cur.execute(
        "SELECT geom, st_area(geom) as area FROM public.lote WHERE name = %s", (reduzido_str,))
    publicLote = cur.fetchone()
    if publicLote is None:
        raise Exception('Geom não encontrado em public.lote')

    # cur.execute("SELECT st_area(geom) FROM public.lote WHERE name = %s", (reduzido_str,))
    area = publicLote['area']
    geom_lote = publicLote['geom']
    if area is None:
        raise Exception('Area da Geom não encontrado em public.lote')

    try:
        cur.execute("INSERT INTO dado_novo.lote (id, setor_cod, quadra_cod, lote_cod, unidade, area_terreno, vago, geom, predial, endereco_id, proprietario_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (lote['id'], lote['setor_cod'], lote['quadra_cod'], lote['lote_cod'], lote['unidade'], area, 's' if vago else 'n', geom_lote, lote['predial'], lote['endereco_id'] if vago else None, lote['proprietario_id'] if vago else None))
        print('Inserindo lote com reduzido=%, area_total=%, endereco_id=%, geom=%',
              lote['id'], area, lote['endereco_id'], geom_lote)
        return True
    except Exception as e:
        print('Erro ao inserir lote em dado_novo.lote Detalhes: %s' % e)
        return False


def cadastraTestada(reduzido, cur):
    try:
        cur.execute(
            "SELECT count(*) FROM dado_novo.testada WHERE lote_id = %s", (reduzido,))
        cont = cur.fetchone()[0]

        cur.execute("SELECT ta.geom FROM public.testada ta, dado_novo.lote l "
                    "WHERE ST_CONTAINS(ST_BUFFER(l.geom,2), ta.geom) AND l.id = %s", (reduzido,))
        testada_geoms = cur.fetchall()

        cur.execute(
            "SELECT logradouro_cod FROM dado_antigo.endereco WHERE id = %s", (reduzido,))
        logradouro_cod = cur.fetchone()[0]

        for testada_geom in testada_geoms:
            cur.execute("SELECT e.id, e.logradouro_nome, e.logradouro_id  FROM dado_novo.eixo e "
                        "WHERE ST_Distance(ST_Centroid(%s),ST_Buffer(e.geom,1,'endcap=flat join=round')) < 100 "
                        "ORDER BY ST_Distance(ST_Centroid(%s),ST_Buffer(e.geom,1,'endcap=flat join=round')) LIMIT 1", (testada_geom, testada_geom))
            eixo = cur.fetchone()

            cont = 1
            if eixo and eixo['logradouro_id'] == logradouro_cod:
                face = 1
            else:
                cont += 1
                face = cont

            cur.execute("INSERT INTO dado_novo.testada (lote_id, eixo_id, geom, face, extensao) "
                        "VALUES (%s, %s, %s, %s, ST_length(%s))", (reduzido, eixo['id'], testada_geom, face, testada_geom))
        return True

    except Exception as e:
        print('Erro ao executar consulta:', e)
        return False


def criarUnidade(reduzido, param, cur):
    try:
        if param is True:
            cur.execute(
                "SELECT * FROM dado_antigo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
            unidades = cur.fetchall()
            for unidade in unidades:
                # Precisará implementar essa função
                ask = consultaCadastraEnd(unidade['id'], cur)
                if ask:
                    cur.execute("INSERT INTO dado_novo.unidade_imobiliaria (id, lote_id, unidade_cod, proprietario_id, endereco_id, setor_cod, quadra_cod, lote_cod) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                                (unidade['id'], unidade['lote_id'], unidade['unidade_cod'], unidade['proprietario_id'], unidade['endereco_id'], unidade['setor_cod'], unidade['quadra_cod'], unidade['lote_cod']))
        else:
            cur.execute("INSERT INTO dado_novo.unidade_imobiliaria (id, lote_id, unidade_cod, proprietario_id, endereco_id, setor_cod, quadra_cod, lote_cod) "
                        "SELECT id, id as lote_id, unidade as unidade_cod, proprietario_id, endereco_id, setor_cod, quadra_cod, lote_cod FROM dado_antigo.lote WHERE id = %s", (reduzido,))

        return True

    except Exception as e:
        print('Erro ao criar unidades imobiliárias em dado_novo.unidade_imobiliaria:', e)
        return False


def CriarEAssociarCobertura(reduzido, cur):
    try:
        reduzido_str = str(reduzido)

        cur.execute("SELECT array_agg(ac.geom) FROM public.lote_ajuste l, public.area_coberta_ajuste ac "
                    "WHERE ST_CONTAINS(l.geom, ac.geom) AND l.name = %s", (reduzido_str,))
        coberturas_temp = cur.fetchone()[0]

        for cobertura_geom in coberturas_temp:
            cur.execute(
                "INSERT INTO dado_novo.area_coberta (geom) VALUES (%s) RETURNING id", (cobertura_geom,))
            cobertura_id = cur.fetchone()[0]

            cur.execute(
                "SELECT id FROM dado_novo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
            unidades = cur.fetchall()

            for unidade in unidades:
                unidade_id = unidade['id']
                cur.execute("INSERT INTO dado_novo.unidade_cobertura (unidade_id, area_coberta_id) "
                            "VALUES (%s, %s)", (unidade_id, cobertura_id))

        return True

    except Exception as e:
        print('Erro ao criar e associar cobertura:', e)
        return False


def consultaCadastraEnd(reduzido, cur):
    try:
        cur.execute(
            "SELECT COUNT(*) FROM dado_novo.endereco WHERE id = %s", (reduzido,))
        count = cur.fetchone()[0]

        if count > 0:
            return True
        else:
            cur.execute(
                "SELECT * FROM dado_antigo.endereco WHERE id = %s", (reduzido,))
            endCopia = cur.fetchone()

            if endCopia['id'] is not None:
                logId = None
                if endCopia['logradouro_cod'] is not None:
                    cur.execute(
                        "SELECT id FROM dado_novo.logradouro   WHERE cod = %s", (endCopia['logradouro_cod'],))
                    logId = cur.fetchone()

                if logId is not None:
                    cur.execute("INSERT INTO dado_novo.endereco (id, logradouro, bairro, numero, complemento) "
                                "VALUES (%s, %s, %s, %s, %s)",
                                (reduzido, logId, endCopia['bairro'], endCopia['numero'], endCopia['complemento']))
                else:
                    cur.execute("INSERT INTO dado_novo.endereco (id, bairro, numero, complemento) "
                                "VALUES (%s, %s, %s, %s)",
                                (reduzido, endCopia['bairro'], endCopia['numero'], endCopia['complemento']))
                return True
            else:
                return False

    except Exception as e:
        print('Erro ao consultar e cadastrar endereço:', e)
        return False
