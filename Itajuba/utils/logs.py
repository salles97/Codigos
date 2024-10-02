class Logs:
    def __init__(self): 
        self.semCoberturas = []
        self.logradourosNaoMapeados = []
        self.alteracaoArea = []
        self.rotuloNaoIdentificado = []
        self.novaUnidade = []  
        self.quadraAtualizada = []
        self.semCoberturas = []
         

    def add_sem_cobertura(self, message):
        self.semCoberturas.append(message)

    def add_logradouro_nao_mapeado(self, message):
        self.logradourosNaoMapeados.append(message)

    def add_alteracao_area(self, message):
        self.alteracaoArea.append(message)

    def add_rotulo_nao_identificado(self, message):
        self.rotuloNaoIdentificado.append(message)
    
    def add_nova_unidade(self, message):
        self.novaUnidade.append(message)

    def add_quadra_atualizada(self, message):
        self.quadraAtualizada.append(message)

    def write_to_file(self, file_path):
        with open(file_path, 'a') as f: 
            f.write("\Imóveis que sofreram mudanca na area:\n")
            for message in self.alteracaoArea:
                f.write(message + '\n')
            f.write("\nRotulos nao encontrados:\n")
            for message in self.rotuloNaoIdentificado:
                f.write(message + '\n')
            f.write("\nImoveis 'CONSTRUIDOS' mas sem coberturas vetorizadas:\n")
            for message in self.semCoberturas:
                f.write(message + '\n')
            f.write("\nLogradouros nao mapeados na vetorização:\n")
            for message in self.logradourosNaoMapeados:
                f.write(message + '\n')
            f.write("\nImoveis onde identificou Novas unidades (Imoveis Vagos com cobertura vetorizada):\n")
            for message in self.novaUnidade:
                f.write(message + '\n')
            f.write("\nImoveis que tiveram a inscricao cadastral atualizada:\n")
            for message in self.quadraAtualizada:
                f.write(message + '\n')