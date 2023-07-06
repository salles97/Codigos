
def carregar_endereco(reduzido, cur):
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
                # Mudar o banco para logradouro id ser o mesmo que o logradouro cod, reduz uma etapa aqui futuramente.
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
                    print(
                        'Geometria do logradouro para este endereço não foi encontrado. Código do logradouro no cadastro:', endCopia['logradouro_cod'])
                return True
            else:
                return False

    except Exception as e:
        print('Erro ao consultar e cadastrar endereço:', e)
        return False
