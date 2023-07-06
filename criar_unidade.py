# Quando predial nao atualizar area

from carregar_endereco import carregar_endereco


def criar_unidade(reduzido, param, cur):
    try:
        if param is True:
            cur.execute(
                "SELECT * FROM dado_antigo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
            unidades = cur.fetchall()
            for unidade in unidades:

                ask = carregar_endereco(unidade['id'], cur)
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
