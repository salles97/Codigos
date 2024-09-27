import traceback  

def carregar_endereco(reduzido, nome, cur, arquivo_log, log):
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
                        "SELECT id FROM dado_novo.logradouro WHERE cod = %s", (endCopia['logradouro_cod'],))
                    logId = cur.fetchone()

                if logId is not None:
                    cur.execute("INSERT INTO dado_novo.endereco (id, logradouro_id  , bairro_cod, numero, complemento) "
                                "VALUES (%s, %s, %s, %s, %s)",
                                (reduzido, logId[0], endCopia['bairro_cod'], endCopia['numero'], endCopia['complemento']
                                 ))
                else:
                    # adversidade_lote(cur, endCopia, 'END', arquivo_log)
                    # cur.execute("INSERT INTO dado_novo.endereco (id, bairro_cod, numero, complemento) "
                    #             "VALUES (%s, %s, %s, %s)",
                    #             (reduzido, endCopia['bairro_cod'], endCopia['numero'], endCopia['complemento']))
                    # arquivo_log.write(f'Logradouro do endereco do imovel {reduzido} nao foi encontrado. Logradouro no cadastro da prefeitura: cod {endCopia["logradouro_cod"]}, nome {endCopia["logradouro"]}\n')
                    log.add_logradouro_nao_mapeado(f'Logradouro do endereco do imovel {nome} nao foi encontrado. Logradouro no cadastro da prefeitura: cod {endCopia["logradouro_cod"]}, nome {endCopia["logradouro"]}')
                    return False
                return True
            else:
                return False

    except Exception as e:
        erro_detalhado = traceback.format_exc()
        arquivo_log.write(f'Erro ao consultar e cadastrar endereco: {e}\n')
        arquivo_log.write(f'Traceback:\n{erro_detalhado}\n')
        return False
