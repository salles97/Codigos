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
from utils.logs import Logs


log = Logs()
def carregar_lote_e_dependencias(con, cur, setor_carga):

        # Criar objeto de log
    with open(f'relatorio_{setor_carga}_carga.txt', 'w') as arquivo_log:
        arquivo_log.write("Iniciando processamento de lotes\n")
        cur.execute("SELECT *, st_area(geom) FROM public.lote")
        lotes = cur.fetchall()
        arquivo_log.write(f"Total de lotes encontrados: {len(lotes)}\n\n")

        for lote in lotes:
            try:
                # arquivo_log.write(f"Processando lote: {lote['name']}\n")
                # Inicia uma nova transação para cada lote
                with con:  # Isso gerencia a transação automaticamente
                    nome = lote['name']
                    # arquivo_log.write(f"Lote Atual: {lote.get('name')}\n")

                    # Atualizar regex para capturar o padrão com múltiplas geometrias
                    padrao_valido = r"^(\d{1,4})-(\d{1,4})-(\d{1,4})(?:-\w*)?(?:\((\d+)\))?$"
                    padrao_nao_identificado = r"^(\d{1,4})-(\d{1,4})-L_s\.cad \(\d+\)$"
                    # padrao_remembramento = r"^\d{1,4}-\d{1,4}-(\d{1,4}(/\d{1,4})+)-R$"                    # Padrão atualizado para capturar setor, quadra e lotes remembrados
                    # padrao_remembramento = r"^(\d{1,4})-(\d{1,4})-(\d{1,4}(/\d{1,4})+)-R$"
                    # Padrão atualizado para capturar setor, quadra e lotes remembrados
                    padrao_remembramento = r"^(\d{1,4})-(\d{1,4})-(.+)-R$"
                    padrao_quadra_atualizada = r"^(\d{1,4})-(\d{1,4})-(\d{1,4})\s*\((?:antigo|antiga|ant\.?|)?\s*(\d{1,4})-(\d{1,4})-(\d{1,4})(?:/\d{1,4})*(?:-R)?\)$"
                    padrao_quadra_atualizada_remembramento = r"^(\d{1,4})-(\d{1,4})-(\d{1,4})\s*\((?:antigo|antiga|ant\.?|)?\s*(\d{1,4})-(\d{1,4})-(\d{1,4}(?:/\d{1,4})*-R)\)$"

# ... resto do código ...
                    # 1. Lotes não identificados
                    if re.match(padrao_nao_identificado, nome):
                        match = re.match(padrao_nao_identificado, nome)
                        setor, quadra = match.groups()[:2]
                        adversidade_lote(cur, lote, 'I', arquivo_log, setor_carga, setor=setor, quadra=quadra)
                        continue

                    # 2. Remembramentos
                    elif re.match(padrao_remembramento, nome):
                        match = re.match(padrao_remembramento, nome)
                        setor, quadra, lotes_remembramento = match.groups()
                        lotes_remembramento = lotes_remembramento.split('/')
                        adversidade_lote(cur, lote, 'R', arquivo_log, setor_carga, setor=setor, quadra=quadra, lotes_remembramento=lotes_remembramento)
                        continue
    
                    elif re.match(padrao_quadra_atualizada_remembramento, nome):
                        match = re.match(padrao_quadra_atualizada_remembramento, nome)
                        novo_setor, nova_quadra, novo_lote, antigo_setor, antiga_quadra, antigo_lote_remembramento = match.groups()
                        lotes_remembramento = antigo_lote_remembramento.split('/')
                        # arquivo_log.write(f"Lote com quadra atualizada e remembramento detectado: {nome}\n")
                        log.add_quadra_atualizada(f"Lote com quadra atualizada e remembramento detectado: {nome}")
                        adversidade_lote(cur, lote, 'R', arquivo_log, setor_carga, setor=antigo_setor, quadra=antiga_quadra, novo_setor=novo_setor, nova_quadra=nova_quadra, novo_lote=novo_lote, lotes_remembramento=lotes_remembramento)
                        continue

                    
                    # 3. Lotes com quadra atualizada
                    elif re.match(padrao_quadra_atualizada, nome):
                        match = re.match(padrao_quadra_atualizada, nome)
                        novo_setor, nova_quadra, novo_lote, antigo_setor, antiga_quadra, antigo_lote = match.groups()
                        print('padrao_quadra_atualizada', novo_setor, nova_quadra, novo_lote, antigo_setor, antiga_quadra, antigo_lote)
                        # Buscar informações do lote usando os dados antigos
                        cur.execute("SELECT * FROM dado_antigo.lote WHERE setor_cod = %s AND quadra_cod = %s AND lote_cod = %s",
                                    (antigo_setor, antiga_quadra, antigo_lote))
                        infoLote = cur.fetchone()
                        
                        if infoLote is None:
                            # arquivo_log.write(f"Rótulo do lote antigo {antigo_setor}-{antiga_quadra}-{antigo_lote} não encontrado.\n")
                            log.add_rotulo_nao_identificado(f"Rótulo do lote antigo {antigo_setor}-{antiga_quadra}-{antigo_lote} não encontrado.")
                            adversidade_lote(cur, lote, 'I', arquivo_log, setor_carga, setor=antigo_setor, quadra=antiga_quadra, lote_cod=antigo_lote, novo_setor=novo_setor, nova_quadra=nova_quadra, novo_lote=novo_lote)
                            continue
                        
                        reduzido = infoLote['id']
                        
                        # Continuar o processamento usando os novos dados de setor, quadra e lote
                        processar_lote(cur, con, arquivo_log, setor_carga, lote, nome, antigo_setor , antiga_quadra, antigo_lote, 
                                       novo_setor, nova_quadra, novo_lote) 
                        
                    
                    # 4. Lotes validos
                    elif re.match(padrao_valido, nome):
                        print('padrao_valido', nome)
                        novo_setor, nova_quadra, novo_lote = None, None, None
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

                        # Verificar se o lote é especial (AV, AI, APP ou AR)
                        match_especial = re.match(r"(\d{1,4})-(\d{1,4})-(\d{1,4})-(AV|AI|APP|AR)", nome)
                        tipo_especial = None
                        if match_especial:
                            tipo_especial = match_especial.group(4)
                            # arquivo_log.write(f'Lote especial {nome} do tipo {tipo_especial}\n')
  
                        # Continuar o processamento com o lote unificado
                        processar_lote(cur, con, arquivo_log, setor_carga, lote, nome,
                                       partes[0], partes[1], partes[2], novo_setor, nova_quadra, novo_lote, tipo_especial)

                    else:
                        # arquivo_log.write(f"Rotulo {nome} nao se encaixa em nenhum padrao conhecido.\n")
                        log.add_rotulo_nao_identificado(f"Rotulo {nome} nao se encaixa em nenhum padrao conhecido.")
                        adversidade_lote(cur, lote, 'I', arquivo_log, setor_carga)
                
            except Exception as e:
                arquivo_log.write(f"Erro não tratado ao processar lote {nome}: {str(e)}\n")
                arquivo_log.write(traceback.format_exc())

        # Não é necessário fechar a conexão e o cursor aqui se eles foram criados fora desta função

    log.write_to_file(f'relatorio_{setor_carga}_carga.txt')

def processar_lote(cur, con, arquivo_log, setor_carga, lote, nome,
                   setor_cod, quadra_cod, lote_cod, novo_setor=None, nova_quadra=None, novo_lote=None, tipo_especial=None):
    try:
        cur.execute("SELECT * FROM dado_antigo.lote WHERE setor_cod = %s AND quadra_cod = %s AND lote_cod = %s",
                    (setor_cod, quadra_cod, lote_cod))
        infoLote = cur.fetchone()

        if infoLote is None:
            # arquivo_log.write(f"Rotulo do lote {setor_cod}-{quadra_cod}-{lote_cod} não encontrado.\n")
            log.add_rotulo_nao_identificado(f"Rotulo do lote {setor_cod}-{quadra_cod}-{lote_cod} não encontrado.")
            adversidade_lote(cur, lote, 'I', arquivo_log, setor_carga, setor=setor_cod, quadra=quadra_cod, lote_cod=lote_cod)
            return 

        reduzido = infoLote['id']

        # Validação da área do lote
        if lote['st_area'] > infoLote['area_terreno'] * 1.3 or lote['st_area'] < infoLote['area_terreno'] * 0.7:
            # arquivo_log.write(f"Lote {reduzido} sofreu mudanca na area, de (registro): {infoLote['area_terreno']} para: {lote['st_area']} (area geometria)\n")
            log.add_alteracao_area(f"Lote {nome} sofreu mudanca na area, de (registro): {infoLote['area_terreno']} para: {lote['st_area']} (area geometria)")
        hasAddres = carregar_endereco(reduzido, nome, cur , arquivo_log, log)
        if not hasAddres: 
            # arquivo_log.write(f"Endereço não pode ser cadastrado para o lote {nome}, reduzido {reduzido}\n")
            adversidade_lote(cur, lote, 'End', arquivo_log, setor_carga, setor=setor_cod, quadra=quadra_cod, lote_cod=lote_cod)
            return    

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
            # arquivo_log.write(f'O lote {nome} não possui unidade imobiliaria e foi identificado cobertura pela vetorização.\n')
                 
            log.add_nova_unidade(f"O lote {nome} não possui unidade imobiliaria e foi identificado cobertura pela vetorização.")
            adversidade_lote(cur, lote, 'Vago', arquivo_log, setor_carga, setor=setor_cod, quadra=quadra_cod, lote_cod=lote_cod)
            # //Criar a unidade com base no lote
            
            
            return   

        # Advsersidade onde não é possível associar cada cobertura a cada imóvel
        elif unidades > 1 and cobertura_geom is not None and len(cobertura_geom) > 1 and infoLote['predial'] == 'Não':
            adversidade_lote(cur, lote, 'C', arquivo_log, setor_carga, setor=setor_cod, quadra=quadra_cod, lote_cod=lote_cod)
            return

        # Lote tem unidade(s) presente na base da prefeitura e foi identificado cobertura(s) e/ou piscina(s) em sua geolocalização
        elif unidades > 0 and ((cobertura_geom is not None and len(cobertura_geom) > 0) or (benfeitoria_geom is not None and len(benfeitoria_geom) > 0)):
            if cadastra_lote(reduzido, False, nome, cur, arquivo_log, log, novo_setor, nova_quadra, novo_lote):
                if cadastra_testada(reduzido, cur, arquivo_log): 
                    
                    try:
                        cur.execute(
                            "SELECT * FROM dado_novo.testada WHERE face = 1 and lote_id = %s", (reduzido,))
                        testada_principal = cur.fetchall()
                        # arquivo_log.write(f'Número de testadas principais encontradas: {len(testada_principal)}\n')

                        if len(testada_principal) == 0:
                            # arquivo_log.write(f'Adversidade de endereço para o lote {nome}\n')
                            con.rollback()
                            adversidade_lote(cur, lote, 'End', arquivo_log, setor_carga, setor=setor_cod, quadra=quadra_cod, lote_cod=lote_cod)
                            return

                        # arquivo_log.write('Encontrou testada principal\n')
                        
                        # Uma área Especial pode ter registro de lote, cadastra ele

                        if tipo_especial:
                            cadastrar_area_especial_lote(
                                cur, lote, reduzido) 
                            arquivo_log.write('Cadastrou area especial\n')

                        # Se não tem adversidade endereço segue a criação das unidades
                        if criar_unidade(reduzido, nome, False, cur, arquivo_log, log):
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
                            # if infoLote['predial'] == 'Não' and unidades == 1:
                            #     atualiza_area_construida_unidade(
                            #         cur, reduzido)
                            # elif infoLote['predial'] != 'Sim':
                            #         arquivo_log.write(f'Lote {nome} Sem informacao predial, não atualizado área\n')
                            return    

                    except Exception as e:
                        arquivo_log.write(f"Erro após adicionar testada: {str(e)}\n")
                        raise Exception()

        # Lote tem Mais de uma unidade presente na base da prefeitura e NÃO foi identificado coberturas e/ou piscinas em sua geolocalização
        elif unidades >= 0 and ((cobertura_geom is None or len(cobertura_geom) == 0) and (benfeitoria_geom is None or len(benfeitoria_geom) == 0)):
        #    
             # arquivo_log.write(
            #     'O lote %s foi identificado sem coberturas e/ou benfeitorias. Lote tem %s unidades\n' % (nome, unidades))
            if cadastra_lote(reduzido, True, nome, cur, arquivo_log, log):
                if cadastra_testada(reduzido, cur, arquivo_log):
                    # Validar se apresenta adversidade de endereço pelas testadas
                    cur.execute(
                        "SELECT * FROM dado_novo.testada WHERE face = 1 and lote_id = %s", (reduzido,))
                    testada_principal = cur.fetchall()

                    if len(testada_principal) == 0:
                        # arquivo_log.write(f'Adversidade de endereço para o lote {nome}\n')
                        con.rollback()
                        adversidade_lote(cur, lote, 'End', arquivo_log, setor_carga, setor=setor_cod, quadra=quadra_cod, lote_cod=lote_cod)
                        return  
                    # Uma área Especial pode ter registro de lote, cadastra ele
                    if tipo_especial:
                        cadastrar_area_especial_lote(
                            cur, lote, reduzido) 
                        # arquivo_log.write('Cadastrou area especial\n')

                    if criar_unidade(reduzido, nome, True, cur, arquivo_log, log):
                        # Quando o lote é um prédio, as áreas de suas unidades não devem ser atualizadas
                        # if infoLote['predial'] == 'Não' and unidades > 1:
                        #     atualiza_area_construida_unidade(
                        #         cur, reduzido)
                        # elif infoLote['predial'] != 'Sim':

                            # arquivo_log.write(f'Lote {nome} Sem informacao no campo predial, nao atualizado area\n')
                            # log.add_predial(f"Lote {nome} Sem informacao no campo predial, nao atualizado area")
                        return
          
            log.add_sem_cobertura(f"O lote {nome} foi identificado sem coberturas e/ou benfeitorias. Lote tem {unidades} unidades")
          
    except Exception as e:
        arquivo_log.write(f"Erro geral no processamento do lote {nome}: {str(e)}\n")
        arquivo_log.write(traceback.format_exc())
        raise Exception()


