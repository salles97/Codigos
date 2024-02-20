

# Quando a área Especial possui registro de lote
import pandas as pd


def cadastrar_area_especial_lote(cur, lote, reduzido):
    nome = lote['name']  # assumindo que 'name' é o nome da coluna
    partes = nome.split('-')

    cur.execute("insert into dado_novo.area_especial (geom, setor_cod, quadra_cod, tipo, lote_id) values"
                "(%s, %s, %s, %s, %s)", (lote['geom'], partes[0], partes[1], partes[3], reduzido))


def area_especial(cur):
    # Busca todas areas especiais
    cur.execute(
        'SELECT id, name, ST_AsText(geom, 31983) FROM public.area_especial')
    areas_especiais = cur.fetchall()

    # Converter para DataFrame
    df = pd.DataFrame(areas_especiais, columns=['id', 'name', 'geom'])

    # Acessar colunas usando índices
    for index, area in df.iterrows():
        partes = area['name'].split('-')
    # Para cada area especial
    # for area in areas_especiais:
    #     # S-Q-Tipo
    #     partes = area['name'].split('-')
        # Buscando o tipo da area nessa rotulagem ruim
        if partes[2].find('('):
            partes[2] = partes[2].split('(')[0]

        # Inserir ela nesse setor, quadra e com o tipo dela. Se houver ja cadastro aumenta a geom
        cur.execute('SELECT * FROM dado_novo.area_especial WHERE setor_cod = %s AND quadra_cod = %s',
                    (partes[0], partes[1]))
        if cur.fetchone() is not None:
            cur.execute('UPDATE dado_novo.area_especial SET geom = st_union(geom, ST_GeomFromText(%s, 31983)) WHERE setor_cod = %s AND quadra_cod = %s',
                        (area[2], partes[0], partes[1]))
        else:
            cur.execute('INSERT INTO dado_novo.area_especial (setor_cod, quadra_cod, tipo, geom) VALUES (%s, %s, %s, %s) RETURNING id',
                        (partes[0], partes[1], partes[2], area[2]))
