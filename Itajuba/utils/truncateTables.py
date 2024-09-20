def truncar_tabelas_dado_novo(con, cur):
    """
    Função para limpar (truncar) as tabelas do esquema dado_novo que são populadas
    pela função de carregar lotes e dependências.
    
    :param cur: Cursor da conexão com o banco de dados
    """
    tabelas = [
        'lote',
        'unidade_imobiliaria',
        'testada',
        'area_coberta',
        'area_especial',
        'benfeitoria',
        'endereco'
    ]
    
    for tabela in tabelas:
        try:
            cur.execute(f"TRUNCATE TABLE dado_novo.{tabela} RESTART IDENTITY CASCADE")
            print(f"Tabela dado_novo.{tabela} foi truncada com sucesso.")
        except Exception as e:
            print(f"Erro ao truncar a tabela dado_novo.{tabela}: {str(e)}")

    print("Processo de limpeza das tabelas dado_novo concluído.")

def truncar_tabelas_to_review(con, cur):
    
    """
    Função para limpar (truncar) as tabelas do esquema to_review.
    
    :param cur: Cursor da conexão com o banco de dados
    """
    tabelas = [
        'lote',
        'area_coberta', 
        'testada'
    ]
    
    for tabela in tabelas:
        try:
            cur.execute(f"TRUNCATE TABLE to_review.{tabela} CASCADE")
            print(f"Tabela to_review.{tabela} foi truncada com sucesso.")
        except Exception as e:
            print(f"Erro ao truncar a tabela to_review.{tabela}: {str(e)}")

    print("Processo de limpeza das tabelas to_review concluído.")

