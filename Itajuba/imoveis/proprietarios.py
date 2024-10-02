def cadastrar_proprietario(reduzido, id, cur, arquivo_log):
    try:
        # Consulta o proprietário na tabela dado_antigo
        cur.execute("SELECT * FROM dado_antigo.proprietario WHERE id = %s", (id,))
        proprietario = cur.fetchone()

        if proprietario is not None:
            # Insere o proprietário na tabela dado_novo
            cur.execute("""
                INSERT INTO dado_novo.proprietario (id, nome, cod, cpf_cnpj, tipo)
                VALUES (%s, %s, %s, %s, %s) ON conflict do nothing
            """, (proprietario['id'], proprietario['nome'], proprietario['cod'], proprietario['cpf_cnpj'], proprietario['tipo']))

            # arquivo_log.write(f'Proprietário {proprietario["nome"]} cadastrado com sucesso.\n')
        else:
            arquivo_log.write(f'Nenhum proprietário encontrado para o lote {reduzido}.\n')

    except Exception as e:
        arquivo_log.write(f'Erro ao cadastrar proprietário para o lote {reduzido}: {str(e)}\n')
