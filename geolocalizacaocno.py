# este código foi criado com o intuito de trabalhar com tabelas do CNO (CADASTRO NACIONAL DE OBRAS)
# realizando a geocodificação através da API do Google Maps utilizando de uma coluna 
#de endereços previamente editada com o intuito de se deixar no padrão do Google.
# **** ELABORADO POR EDUARDO ADRIANI RAPANOS em 13/06/2023 ****
# contato: earapanos@gmail.com
# Todos os direitos reservados


import psycopg2
import requests

# função que define a conecção com o banco de dados
def conectar_banco_dados(host, database, user, password):
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        print('Conexão bem-sucedida!')
        return connection
    except psycopg2.Error as e:
        print('Erro ao conectar-se ao banco de dados:', e)
        return None

# aqui é definida a função que adiciona as coordenadas de latitude e longitude no DB    
def geocodificar_endereco(endereco, api_key):
    url = f'https://maps.googleapis.com/maps/api/geocode/json?address={endereco}&bounds=-33.750710,-73.985535|-7.816132,-34.793979&key={api_key}&timeout=10000'
    response = requests.get(url)
    data = response.json()

    if data['status'] == 'OK':
        result = data['results'][0]
        latitude = result['geometry']['location']['lat']
        longitude = result['geometry']['location']['lng']
        precisao = result['geometry']['location_type']
        endereco_formatado = result['formatted_address']
        return latitude, longitude, precisao, endereco_formatado
    else:
        print(f'Erro ao geocodificar o endereço: {endereco}')
        return None, None, None, None
    
# aqui é definida a função que atualiza tabela com as novas informações sobre a geocodificação no DB
def atualizar_tabela_geocodificada(connection, schema, tabela, endereco_column, precisao_column, endereco_formatado, api_key):
    cursor = connection.cursor()

    query = f"SELECT {endereco_column}, latitude, longitude FROM {schema}.{tabela} WHERE nm_mun = 'nome1muni' OR nm_mun = 'nome2muni' OR nm_mun = 'nome3muni' OR nm_mun = 'nome4muni'"
    cursor.execute(query)
    enderecos = cursor.fetchall()

    # verifica se a latitude/longitude já estão preenchidas. Se sim, ele não roda o geocoding.
    for endereco in enderecos:
        endereco, latitude, longitude = endereco[0], endereco[1], endereco[2]
        
        if latitude is None or longitude is None:
            latitude, longitude, precisao, endereco_formatado = geocodificar_endereco(endereco, api_key)

            if latitude is not None and longitude is not None:
                endereco_formatado = endereco_formatado.replace("'", " ")  # substituir aspas simples por espaço para não dar BO no geocoding
                update_query = f'UPDATE {schema}.{tabela} SET latitude = {latitude}, longitude = {longitude}, {precisao_column} = \'{precisao}\', endereco_formatado = \'{endereco_formatado}\' WHERE {endereco_column} = %s'
                cursor.execute(update_query, (endereco,))
                connection.commit()
                print(f'Endereço: {endereco}, Latitude: {latitude}, Longitude: {longitude}, Precisão: {precisao}, Endereço Formatado: {endereco_formatado}')
        else:
            print(f'Endereço: {endereco} já possui latitude e longitude')

    cursor.close()


# aqui a conecção do banco é encerrada se não há correspondência da tabela no DB
def fechar_conexao(connection):
    if connection is not None:
        connection.close()

# Configurações de conexão com o banco de dados
host = 'seuhost'
database = 'suadatabase'
user = 'seuuser'
password = 'suasenha'

# aqui é inserida a chave da API do google
api_key = 'suaapi'

# aqui vamos conectar ao banco de dados
connection = conectar_banco_dados(host, database, user, password) 

if connection is not None:
    # aqui vamos atualizar a view geocodificada
    schema = 'seuschema'
    tabela = 'suatabela'
    endereco_column = 'suacolna'
    precisao_column = 'suacolunaprecision'
    endereco_formatado = 'seuenderecoformat'

    atualizar_tabela_geocodificada(connection, schema, tabela, endereco_column, precisao_column, endereco_formatado, api_key)

    # ao finalizar, vamos fechar a conecção
    fechar_conexao(connection)