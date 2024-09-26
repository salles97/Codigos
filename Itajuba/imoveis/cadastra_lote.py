def cadastra_lote(reduzido, vago, nome, cur, arquivo_log, log, novo_setor=None, nova_quadra=None, novo_lote=None):

    # Buscará pelo registro do lote na base da prefeitura
    cur.execute("SELECT * FROM dado_antigo.lote WHERE id = %s", (reduzido,))
    lote = cur.fetchone()
    if lote is None:
        raise Exception('Lote não encontrado em dado_antigo.lote')

#   Vai buscar a geometria e a area dela
    cur.execute(
        "SELECT geom, st_area(geom) as area FROM public.lote WHERE name = %s", (nome,))
    publicLote = cur.fetchone()
    if publicLote is None:
        raise Exception('Geom não encontrado em public.lote')

    area = publicLote['area']
    geom_lote = publicLote['geom']
    if area is None:
        raise Exception('Area da Geom não encontrado em public.lote')

    try:
        if novo_setor and nova_quadra and novo_lote:
            lote['setor_cod'] = novo_setor
            lote['quadra_cod'] = nova_quadra
            lote['lote_cod'] = novo_lote
            # arquivo_log.write(f"Lote {nome} atualizado com setor {novo_setor}, quadra {nova_quadra} e lote {novo_lote}\n")
            log.add_quadra_atualizada(f"Lote {nome} atualizado com setor {novo_setor}, quadra {nova_quadra} e lote {novo_lote}")

        cur.execute("INSERT INTO dado_novo.lote (id, setor_cod, quadra_cod, lote_cod, unidade, area_terreno, vago, geom, predial, endereco_id, proprietario_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (lote['id'], lote['setor_cod'], lote['quadra_cod'], lote['lote_cod'], lote['unidade'], area, 's' if vago else 'n', geom_lote, lote['predial'], lote['endereco_id'] if vago else None, lote['proprietario_id'] if vago else None))
        print('Inserindo lote com reduzido=%, area_terreno=%, endereco_id=%, geom=%',
              lote['id'], area, lote['endereco_id'], geom_lote)
        return True
    except Exception as e:
        print('Erro ao inserir lote em dado_novo.lote Detalhes: %s' % e)
        return False
