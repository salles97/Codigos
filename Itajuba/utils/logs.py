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
            f.write("\nMensagens sobre mudanca na area:\n")
            for message in self.alteracaoArea:
                f.write(message + '\n')
            f.write("\nRótulos não encontrados:\n")
            for message in self.rotuloNaoIdentificado:
                f.write(message + '\n')
            f.write("\nIdentificados sem coberturas:\n")
            for message in self.semCoberturas:
                f.write(message + '\n')
            f.write("\nLogradouros não mapeados:\n")
            for message in self.logradourosNaoMapeados:
                f.write(message + '\n')
            f.write("\nNovas unidades:\n")
            for message in self.novaUnidade:
                f.write(message + '\n')
            f.write("\nQuadras atualizadas:\n")
            for message in self.quadraAtualizada:
                f.write(message + '\n')