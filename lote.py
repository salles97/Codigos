import psycopg2

from adversidade_lote import adversidade_lote
from cadastra_lote import cadastra_lote
from cadastra_testada import cadastra_testada
from calculo_areas import atualiza_area_construida_unidade
from carregar_cobertura import carregar_cobertura
from carregar_endereco import carregar_endereco
from criar_unidade import criar_unidade


def carregar_lote_e_dependencias(con, cur):
    cur.execute("SELECT *, st_area(geom) FROM public.lote")
    lotes = cur.fetchall()

    for lote in lotes:
        nome = lote['name']  # assumindo que 'name' é o nome da coluna
        partes = nome.split('-')

        if len(partes) == 4 or partes[0].startswith("Property"):
            adversidade_lote(cur, lote)
            con.commit()
            continue

        elif len(partes) == 3:
            if partes[2].endswith(")"):
                # o lote multi parte será encontrado em public? após o primeiro passar aqui, os demais são removidos
                cur.execute(
                    "SELECT * FROM public.lote WHERE name LIKE %s", (nome))
                have_lote = cur.fetchone()
                if have_lote is not None:
                    # Encontrar todos os lotes com a mesma estrutura antes dos parênteses

                    cur.execute(
                        "SELECT * FROM public.lote WHERE name LIKE %s", ('{}%'.format(nome[:-3]),))
                    lotes_multiplos = cur.fetchall()

                    if len(lotes_multiplos) > 0:
                        cur.execute(
                            "SELECT st_union(geom) FROM public.lote WHERE name LIKE %s", ('{}%'.format('-'.nome[:-3]),))
                        multi_geom = cur.fetchone()
                        # Realizar a união das geometrias de todos os lotes
                        cur.execute(
                            "UPDATE public.lote SET geom = %s WHERE name = %s", (multi_geom, nome))

                        # remove o sufixo para carregar as dependencias do lote abaixo
                        partes[2] = partes[2][:-3]
                        lotes_unificados = [
                            lote['name'] for lote in lotes_multiplos if lote['name'] != nome]

                        # Deletar as outras partes do lote
                        for nome_unificado in lotes_unificados:
                            cur.execute(
                                "DELETE FROM public.lote WHERE name = %s", (nome_unificado,))

                        # con.commit()

            if partes[2].isdigit():
                try:
                    # Verifica se a rotulagem corresponde a algum registro de lote.
                    cur.execute("SELECT * FROM dado_antigo.lote WHERE setor_cod = %s AND quadra_cod = %s AND lote_cod = %s",
                                (partes[0], partes[1], partes[2]))
                    infoLote = cur.fetchone()
                    if infoLote is None:
                        print('Lote não encontrado em dado_antigo.lote')
                        con.commit()
                        continue
                    reduzido = infoLote['id']

                    # Se carrega o endereço do lote, sempre deve carregar, mas é possível identificar erro com o logradouro do lote.
                    # Esses endereços com 'erros' são cadastrados sem referenciar logradouro.
                    hasAddres = carregar_endereco(reduzido, cur)
                    if not hasAddres:
                        raise Exception('Endereço não pode ser cadastrado')

                    # Consultar quantas unidades existem para o lote
                    cur.execute(
                        "SELECT count(*) FROM dado_antigo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
                    unidades = cur.fetchone()[0]

                    cur.execute("SELECT ac.geom FROM public.lote l, public.area_coberta ac "
                                "WHERE st_contains(l.geom, ac.geom) AND l.name = %s", (nome,))
                    cobertura_geom = cur.fetchone()

                    cur.execute("SELECT b.geom FROM public.lote l, public.benfeitoria b "
                                "WHERE st_contains(l.geom, b.geom) AND l.name = %s", (nome,))
                    benfeitoria_geom = cur.fetchone()

                    # Lote não tem unidade presente na base da prefeitura mas foi identificado cobertura e/ou piscinas em sua geolocalização
                    if unidades == 0 and (cobertura_geom is not None or benfeitoria_geom is not None):
                        if cadastra_lote(reduzido, True, cur):
                            print(
                                'Unidade não existe em nossa base, criada unidade a partir das informações do lote: ', reduzido)
                            if cadastra_testada(reduzido, cur):
                                if criar_unidade(reduzido, True, cur):
                                    if carregar_cobertura(reduzido, cur):
                                        atualiza_area_construida_unidade(
                                            cur, reduzido)
                                        con.commit()
                                        continue
                    # Lote tem unidade(s) presente na base da prefeitura e foi identificado cobertura(s) e/ou piscina(s) em sua geolocalização
                    elif unidades > 0 and (cobertura_geom is not None or benfeitoria_geom is not None):
                        if cadastra_lote(reduzido, False, cur):
                            if cadastra_testada(reduzido, cur):
                                # Validar se é adversidade????
                                if criar_unidade(reduzido, False, cur):
                                    if carregar_cobertura(reduzido, cur):
                                        #       Quando o lote é um predio, as áreas de suass unidades não devem ser atualizadas
                                        if infoLote['predial'] == 'Não':
                                            atualiza_area_construida_unidade(
                                                cur, reduzido)
                                        elif infoLote['predial'] != 'Sim':
                                            print(
                                                'Sem informação no campo predial, não atualizado area')
                                        con.commit()
                                        continue
                    # Lote tem Mais de uma unidade presente na base da prefeitura e NÃO  foi identificado coberturas e/ou piscinas em sua geolocalização
                    elif unidades >= 0 and (cobertura_geom is None and benfeitoria_geom is None):
                        if cadastra_lote(reduzido, False, cur):
                            if cadastra_testada(reduzido, cur):
                                # Caso de predial? Caso de adversidade tipo C
                                print(
                                    'O lote possui unidades mas não possui coberturas vetorizadas' % reduzido)
                                con.commit()
                                continue

                except Exception as e:
                    print(e)
                    return False
            else:
                # Tratativa para quando é um polígono composto por múltiplos polígonos
                print('Nao é digito e nem multi partes')

        else:
            print(nome)

    return False
