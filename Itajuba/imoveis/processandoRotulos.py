import re
import psycopg2
from imoveis.adversidade_lote import adversidade_lote
from imoveis.area_especial import cadastrar_area_especial_lote
from imoveis.cadastra_lote import cadastra_lote
from imoveis.cadastra_testada import cadastra_testada
from imoveis.calculo_areas import atualiza_area_construida_unidade
from imoveis.carregar_benfeitoria import carregar_benfeitoria
from imoveis.carregar_cobertura import carregar_cobertura
from imoveis.carregar_endereco import carregar_endereco
from imoveis.criar_unidade import criar_unidade
import traceback

def carregar_lote_e_dependencias(con, cur, setor_carga):
    with open(f'relatorio_{setor_carga}_carga.txt', 'w') as arquivo_log:
        arquivo_log.write("Iniciando processamento de lotes\n")
        cur.execute("SELECT *, st_area(geom) FROM public.lote")
        lotes = cur.fetchall()
        arquivo_log.write(f"Total de lotes encontrados: {len(lotes)}\n")

        for lote in lotes:
            try:
                arquivo_log.write(f"Processando lote: {lote['name']}\n")
                # Inicia uma nova transação para cada lote
                with con:  # Isso gerencia a transação automaticamente
                    nome = lote['name']
                    # arquivo_log.write(f"Lote Atual: {lote.get('name')}\n")

                    # Atualizar regex para capturar o padrão com múltiplas geometrias
                    padrao_valido = r"(\d{1,4})-(\d{1,4})-(\d{1,4})(?:-\w*)?(?: \((\d+)\))?"
                    padrao_nao_identificado = r"^\d{1,4}-\d{1,4}-L_s\.cad \(\d+\)$"
                    padrao_remembramento = r"^\d{1,4}-\d{1,4}-(\d{1,4}(/\d{1,4})+)-R$"
                    padrao_quadra_errada = r"^\d{1,4}-\d{1,4}-\d{1,4} \(antigo \d{1,4}-\d{1,4}-\d{1,4}\)$"

                    # 1. Lotes não identificados
                    if re.match(padrao_nao_identificado, nome):
                        adversidade_lote(cur, lote, 'I', arquivo_log)
                        continue

                    # 2. Remembramentos
                    elif re.match(padrao_remembramento, nome):
                        adversidade_lote(cur, lote, 'R', arquivo_log)
                        continue

                    # 3. Quadra errada
                    elif re.match(padrao_quadra_errada, nome,):
                        # arquivo_log.write(f"Lote {nome} identificado com quadra errada.\n")
                        continue

                    # 4. Lotes com múltiplas geometrias
                    elif re.match(padrao_valido, nome):
                       

                        partes = re.match(padrao_valido, nome).groups()
                        # Verificar se o lote tem múltiplas geometrias (exemplo: "02-01-71 (1)")
                        match_multiparte = re.match(r"(\d{1,4})-(\d{1,4})-(\d{1,4})(\s*\((\d+)\))?", nome)

                        if match_multiparte and match_multiparte.group(5):  # Verifica se há um número entre parênteses
                            print(match_multiparte.groups())
                            arquivo_log.write(f'Lote multi parte {nome} \n')
                            base_nome = f"{match_multiparte.group(1)}-{match_multiparte.group(2)}-{match_multiparte.group(3)}"
                            parte_numero = match_multiparte.group(5)

                            # Verificar e unificar a geometria das partes do lote
                            cur.execute(
                                "SELECT * FROM public.lote WHERE name LIKE %s", ('{}-{}-{}%'.format(partes[0], partes[1], partes[2]),))
                            lotes_multiplos = cur.fetchall()

                            if len(lotes_multiplos) > 1:
                                # Realizar a união das geometrias de todos os lotes
                                cur.execute(
                                    "SELECT ST_Union(ARRAY[st_union(geom)]) FROM public.lote WHERE name LIKE %s", ('{}-{}-{}%'.format(partes[0], partes[1], partes[2] + '%'),))
                                multi_geom = cur.fetchone()[0]

                                # Atualizar a geometria do primeiro lote com a geometria unificada
                                cur.execute(
                                    "UPDATE public.lote SET geom = %s WHERE name = %s", (multi_geom, nome))

                                # Deletar as outras partes do lote
                                lotes_unificados = [
                                    lote['name'] for lote in lotes_multiplos if lote['name'] != nome]

                                for nome_unificado in lotes_unificados:
                                    cur.execute(
                                        "DELETE FROM public.lote WHERE name = %s", (nome_unificado,))

                        #  # Verificar se o nome do lote tem sufixos de áreas especiais
                        # sufixos_areas_especiais = ['AI', 'APP', 'AV', 'AR']
                        # for sufixo in sufixos_areas_especiais:
                        #     if nome.endswith(sufixo):
                        #         cadastrar_area_especial_lote(cur, lote['id'], sufixo)
                        #         arquivo_log.write(f"Área especial {sufixo} cadastrada para o lote {nome}.\n")
                        #         break

                        # Continuar o processamento com o lote unificado
                        try:
                            cur.execute("SELECT * FROM dado_antigo.lote WHERE setor_cod = %s AND quadra_cod = %s AND lote_cod = %s",
                                        (partes[0], partes[1], partes[2]))
                            infoLote = cur.fetchone()

                            if infoLote is None:
                                arquivo_log.write(f"Rótulo do lote {partes[0]}-{partes[1]}-{partes[2]} não encontrado.\n")
                                continue

                            reduzido = infoLote['id']

                            # Validação da área do lote
                            if lote['st_area'] > infoLote['area_terreno'] * 1.3 or lote['st_area'] < infoLote['area_terreno'] * 0.7:
                                arquivo_log.write(f"Lote {reduzido} mudou muito a área, de (registro): {infoLote['area_terreno']} para (área geometria): {lote['st_area']}\n")

                            hasAddres = carregar_endereco(reduzido, cur, arquivo_log)
                            if not hasAddres: 
                                arquivo_log.write(f"Endereço não pode ser cadastrado para o lote {nome}, reduzido {reduzido}\n")
                                adversidade_lote(cur, lote, 'End', arquivo_log)
                                continue    

                            # Consultar quantas unidades existem para o lote
                            cur.execute(
                                "SELECT count(*) FROM dado_antigo.unidade_imobiliaria WHERE lote_id = %s", (reduzido,))
                            unidades = cur.fetchone()[0]

                            cur.execute("SELECT ac.geom FROM public.lote l, public.area_coberta ac "
                                        "WHERE st_contains(l.geom, ac.geom) AND l.name = %s", (nome,))
                            cobertura_geom = cur.fetchall()

                            cur.execute("SELECT b.geom FROM public.lote l, public.benfeitoria b "
                                        "WHERE st_contains(l.geom, b.geom) AND l.name = %s", (nome,))
                            benfeitoria_geom = cur.fetchall()

                            # Lote não tem unidade presente na base da prefeitura mas foi identificado cobertura e/ou piscinas em sua geolocalização
                            if unidades == 0 and ((cobertura_geom is not None and len(cobertura_geom) > 0) or (benfeitoria_geom is not None and len(benfeitoria_geom) > 0)):
                                arquivo_log.write(
                                    'O lote %s não possui unidade imobiliaria e foi identificado cobertura pela vetorização.\n', reduzido)
                                adversidade_lote(cur, lote, 'Vago', arquivo_log)
                                continue

                            # Advsersidade onde não é possível associar cada cobertura a cada imóvel
                            elif unidades > 1 and cobertura_geom is not None and len(cobertura_geom) > 0 and infoLote['predial'] == 'Não':
                                adversidade_lote(cur, lote, 'C', arquivo_log)
                                continue

                            # Lote tem unidade(s) presente na base da prefeitura e foi identificado cobertura(s) e/ou piscina(s) em sua geolocalização
                            elif unidades > 0 and ((cobertura_geom is not None and len(cobertura_geom) > 0) or (benfeitoria_geom is not None and len(benfeitoria_geom) > 0)):
                                if cadastra_lote(reduzido, False,nome, cur):
                                    if cadastra_testada(reduzido, cur, arquivo_log): 
                                        
                                        try:
                                            cur.execute(
                                                "SELECT * FROM dado_novo.testada WHERE face = 1 and lote_id = %s", (reduzido,))
                                            testada_principal = cur.fetchall()
                                            # arquivo_log.write(f'Número de testadas principais encontradas: {len(testada_principal)}\n')

                                            if len(testada_principal) == 0:
                                                arquivo_log.write(f'Adversidade de endereço para o lote {nome}\n')
                                                con.rollback()
                                                adversidade_lote(cur, lote, 'End', arquivo_log)
                                                continue

                                            # arquivo_log.write('Encontrou testada principal\n')
                                            
                                            # Uma área Especial pode ter registro de lote, cadastra ele

                                            if partes[3]:
                                                cadastrar_area_especial_lote(
                                                    cur, lote, reduzido)
                                                partes.pop(3)
                                                arquivo_log.write('Cadastrou area especial\n')

                                            # Se não tem adversidade endereço segue a criação das unidades
                                            if criar_unidade(reduzido, False, cur,arquivo_log):
                                                # arquivo_log.write('Criou unidade\n')

                                                if (cobertura_geom is not None and len(cobertura_geom) > 0):
                                                    # arquivo_log.write(f'Cobertura encontrada para o lote {reduzido}\n')
                                                    # arquivo_log.write(f'Número de geometrias de cobertura: {len(cobertura_geom)}\n')
                                                    carregar_cobertura(reduzido, cur,arquivo_log)
                                                if (benfeitoria_geom is not None and len(benfeitoria_geom) > 0):
                                                    # arquivo_log.write(f'Benfeitoria encontrada para o lote {reduzido}\n')
                                                    # arquivo_log.write(f'Número de geometrias de benfeitoria: {len(benfeitoria_geom)}\n')
                                                    carregar_benfeitoria(reduzido, cur)
                                                
                                                # Quando o lote é um prédio, as áreas de suas unidades não devem ser atualizadas
                                                if infoLote['predial'] == 'Não' and unidades == 1:
                                                    atualiza_area_construida_unidade(
                                                        cur, reduzido)
                                                elif infoLote['predial'] != 'Sim':
                                                       arquivo_log.write(f'Lote {nome} Sem informação no campo predial, não atualizado área\n')
                                                continue    

                                        except Exception as e:
                                            arquivo_log.write(f"Erro após adicionar testada: {str(e)}\n")
                                            raise Exception()

                            # Lote tem Mais de uma unidade presente na base da prefeitura e NÃO foi identificado coberturas e/ou piscinas em sua geolocalização
                            elif unidades >= 0 and ((cobertura_geom is None or len(cobertura_geom) == 0) and (benfeitoria_geom is None or len(benfeitoria_geom) == 0)):
                                # arquivo_log.write(
                                    # 'O lote %s foi identificado sem coberturas e/ou benfeitorias. Lote com %s unidades\n' % (nome, unidades))
                                if cadastra_lote(reduzido, True, nome, cur):
                                    if cadastra_testada(reduzido, cur, arquivo_log):
                                        # Validar se apresenta adversidade de endereço pelas testadas
                                        cur.execute(
                                            "SELECT * FROM dado_novo.testada WHERE face = 1 and lote_id = %s", (reduzido,))
                                        testada_principal = cur.fetchall()

                                        if len(testada_principal) == 0:
                                            adversidade_lote(cur, lote, 'End', arquivo_log)
                                            arquivo_log.write(f'Adversidade de endereço para o lote {nome}\n')
                                            con.rollback()
                                            continue
                                        # Uma área Especial pode ter registro de lote, cadastra ele
                                        if partes[3]:
                                            cadastrar_area_especial_lote(
                                                cur, lote, reduzido)
                                            partes.pop(3)
                                            arquivo_log.write('Cadastrou area especial\n')

                                        if criar_unidade(reduzido, True, cur, arquivo_log):
                                            # Quando o lote é um prédio, as áreas de suas unidades não devem ser atualizadas
                                            if infoLote['predial'] == 'Não' and unidades == 1:
                                                atualiza_area_construida_unidade(
                                                    cur, reduzido)
                                            elif infoLote['predial'] != 'Sim':
                                                arquivo_log.write(f'Lote {nome} Sem informação no campo predial, não atualizado área')
                                            continue

                        except Exception as e:
                            arquivo_log.write(f"Erro geral no processamento do lote {nome}: {str(e)}\n")
                            arquivo_log.write(traceback.format_exc())
                            raise Exception()

                    else:
                        arquivo_log.write(f"Lote {nome} não se encaixa em nenhum padrão conhecido.\n")
                        adversidade_lote(cur, lote, 'Outra', arquivo_log)
                
            except Exception as e:
                arquivo_log.write(f"Erro não tratado ao processar lote {nome}: {str(e)}\n")
                arquivo_log.write(traceback.format_exc())

        # Não é necessário fechar a conexão e o cursor aqui se eles foram criados fora desta função