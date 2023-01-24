#!/usr/bin/env python
# coding: utf-8

# # Script Em Desenvolvimento

# ## Imports

# In[10]:


## Espaço no código para importação de bibliotecas

import os
import xlsxwriter
import pandas as pd
import time
import re
import sys
import numpy as np
import datetime as dt
import requests
import urllib.parse
import csv


# ## Declarando Funções

# In[14]:


## Definição de funções utilizadas no código


## ---------------------- Função de requisição ----------------------------
def buscar_dados_id(cep, num,munic, logra, setor=None): # Função para requisição
    try:
        #print(f'cep {urllib.parse.quote(cep)} num {urllib.parse.quote(num)} munic {urllib.parse.quote(munic)} logra {urllib.parse.quote(logra)}')
        request = requests.get(f"http://w3.sp.ibge.gov.br/index.php?r=ws/cnefe&tipo=setorendereco&key=ABRRFJ65F45DGD65563HGFDG2&cep={urllib.parse.quote(cep)}&num={urllib.parse.quote(num)}&municipio={urllib.parse.quote(munic)}&logradouro={urllib.parse.quote(logra)}", verify=False)
        #print(f'setor {setor} cep{cep} num{num} municipio{munic} logradouro{logra}')
        status= request.status_code
        if(status == 200):
            items = request.json()

            return items
        elif (status == 400):
            return False
        else:
            return False
        
    except request.status_code:
        print('erro')

        
# ---------------------- Função de concatenar dataframe
def concatenar_dataframe_ibge(df, dadosJson, id_chamado): 
    if(dadosJson is False):
        print('0')
        return df
    
    
    data = {'id_chamado':id_chamado,
                        'cod_setor': dadosJson['cod_setor'],
                        'num_quadra': dadosJson['num_quadra'],
                        'num_face': dadosJson['num_face'],
                        #'cod_endereco': dadosJson['cod_endereco'],
                        #'cod_unico_endereco': dadosJson['cod_unico_endereco'],
                        #'seq_uv': dadosJson['seq_uv'],
                        #'num_seq_fisica_qf': dadosJson['num_seq_fisica_qf'],
                        #'dsc_cod_status_end': dadosJson['dsc_cod_status_end'],
                        'nom_tipo_seglogr': dadosJson['nom_tipo_seglogr'],
                        'nom_titulo_seglogr': dadosJson['nom_titulo_seglogr'],
                        'nom_seglogr': dadosJson['nom_seglogr'],
                        'num_endereco': dadosJson['num_endereco'],
                        'dsc_modificador': dadosJson['dsc_modificador'],
                        #'dsc_ponto_referencia': dadosJson['dsc_ponto_referencia'],
                        'val_latitude': dadosJson['val_latitude'],
                        'val_longitude': dadosJson['val_longitude'],
                        #'num_satelites': dadosJson['num_satelites'],
                        #'val_pdop': dadosJson['val_pdop'],
                        #'val_intensidade_sinal': dadosJson['val_intensidade_sinal'],
                        #'dat_confirmacao_fonte': dadosJson['dat_confirmacao_fonte'],
                        #'dsc_ind_existe_identificacao': dadosJson['dsc_ind_existe_identificacao'],
                        #'val_latitude_anterior': dadosJson['val_latitude_anterior'],
                        #'val_longitude_anterior': dadosJson['val_longitude_anterior'],
                        #'dsc_ind_selecionado_pesq': dadosJson['dsc_ind_selecionado_pesq'],
                        #'num_tentativas_coords': dadosJson['num_tentativas_coords'],
                        #'dsc_ind_sucesso_coordenadas': dadosJson['dsc_ind_sucesso_coordenadas'],
                        #'dsc_ind_ponto_dentro_setor': dadosJson['dsc_ind_ponto_dentro_setor'],
                        #'dsc_ind_ponto_dentro_aie_i': dadosJson['dsc_ind_ponto_dentro_aie_i'],
                        #'dsc_ind_ponto_dentro_aie_q': dadosJson['dsc_ind_ponto_dentro_aie_q'],
                        #'cod_endereco_subordinado': dadosJson['cod_endereco_subordinado'],
                        #'num_endereco_ant': dadosJson['num_endereco_ant'],
                        #'dat_atualizacao': dadosJson['dat_atualizacao'],
                        #'dt_inclusao_alteracao_bd_oper': dadosJson['dt_inclusao_alteracao_bd_oper'],
                        #'data_inclusao_bd_analitica': dadosJson['data_inclusao_bd_analitica'],
                        #'geom': dadosJson['geom'],
                        'cep_face': dadosJson['cep_face'],
                        'cod_area': dadosJson['cod_area'],
                        'area': dadosJson['area'],
                        'cod_subarea': dadosJson['cod_subarea'],
                        'subarea': dadosJson['subarea'],
                        'cod_posto': dadosJson['cod_posto'],
                        'posto': dadosJson['posto']}
    df2 = pd.DataFrame([data])
    
    
    
    
    if(df is None):
        return df2
    
    return pd.concat([df, df2], ignore_index=True)
        
    
    
    
def concatenar_dataframe_chamados(df, dados):
    # dados deve ser um dicionario 
    df2 = pd.DataFrame([dados])
    
    if(df is None):
        return df2
    
    return pd.concat([df, df2], ignore_index=True)



def dataframes_to_file(isReq, ext, dfChamados, dfIbge):
    x = dt.datetime.now()
    if(ext == 'xlsx'):
        if(isReq == 'sim'): # se teve requisição
            dfIbge.to_excel(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_ConsultaIbge.xlsx')
        
        dfChamados.to_excel(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Chamados.xlsx')
    elif(ext == 'csv'):
        if(isReq == 'sim'): # se teve requisição
            dfIbge.to_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_ConsultaIbge.csv',quoting=csv.QUOTE_NONNUMERIC, index=False , sep=";")
    
        dfChamados.to_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Chamados.csv',quoting=csv.QUOTE_NONNUMERIC, index=False, sep=";")
    
    
def cruzar_dados(isRec, ext):
    x = dt.datetime.now()
    if(isRec == 'sim'):
        if(ext == 'csv'):
            cha = pd.read_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Chamados.csv', sep=';') # integro left ID
            ibge = pd.read_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_ConsultaIbge.csv', sep=";") # id_chamado
    
    
            csv_final= cha.merge(ibge, left_on='ID', right_on="id_chamado")
    
            csv_final["Endereço"] = csv_final['logradouro'].astype(str) +"\n"+ csv_final["numero"].astype(str) + "\n"+csv_final["complemento"].astype(str)+ "\n"+csv_final["cep"].astype(str)
            pathold = "/csv/"
            dirs = os.listdir( pathold )
            numArquivos = []
            numRodadas = 1
    
            for file in dirs:
                rodadas = re.findall(r'rodada', file)
                numArquivos += [*rodadas]
    
            if(len(numArquivos)):
                numRodadas += len(numArquivos)
                csv_final.to_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Merge_rodada{numRodadas}.csv', sep=';')
            else:
                numRodadas = 1
                csv_final.to_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Merge_rodada{numRodadas}.csv', sep=';')
                
        else:
            cha = pd.read_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Chamados.csv', sep=';') # integro left ID
            ibge = pd.read_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_ConsultaIbge.csv', sep=";") # id_chamado
    
    
            csv_final= cha.merge(ibge, left_on='ID', right_on="id_chamado")
    
            csv_final["Endereço"] = csv_final['logradouro'].astype(str) +"\n"+ csv_final["numero"].astype(str) + "\n"+csv_final["complemento"].astype(str)+ "\n"+csv_final["cep"].astype(str)
            pathold = "/csv/"
            dirs = os.listdir( pathold )
            numArquivos = []
            numRodadas = 1
    
            for file in dirs:
                rodadas = re.findall(r'rodada', file)
                numArquivos += [*rodadas]
    
            if(len(numArquivos)):
                numRodadas += len(numArquivos)
                csv_final.to_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Merge_rodada{numRodadas}.csv', sep=';')
            else:
                numRodadas = 1
                csv_final.to_csv(f'csv/{x.year}-{x.strftime("%m")}-{x.day}_Merge_rodada{numRodadas}.csv', sep=';')


# In[15]:


## execução principal


def funcao_Principal(isRec):
    dfChamados = None
    dfIbge = None

    exportarComo = 'xlsx'
    df = pd.read_csv('glpi.csv', sep=';')
    if(exportarComo == 'xlsx'):
        workbook = xlsxwriter.Workbook('NovaPlanilha.xlsx')
        worksheet = workbook.add_worksheet()
        workbook2 = xlsxwriter.Workbook('NovaPlanilha2.xlsx')
        worksheet2 = workbook2.add_worksheet()

        worksheet.write('A1', 'Data Solicitação') # Data
        worksheet.write('B1', 'Operador') # Requerente
        worksheet.write('C1', 'Registro') # ID
        worksheet.write('D1', 'Morador') # Nome
        worksheet.write('E1', 'Cidade') # municipio
        worksheet.write('F1', 'Endereço') # Logradouro 
        worksheet.write('G1', 'Telefone') # Telefone
        worksheet.write('H1', 'Email') # Email
        worksheet.write('I1', 'Codigo endereço') # Codigo de endereço ????
        worksheet.write('J1', 'Codigo Setor Censitário') # setor
        worksheet.write('K1', 'Observações') # Acompanhamento - Descrição
        worksheet.write('L1', 'Melhor horario para encontrar final de semana') # Final de Semana
        worksheet.write('M1', 'Melhor horario para encontrardurante a semana') # Durante a semana
        



    linha = 1
    erros = 0
    certos = 0

    for i in range(0, len(df)):
        linha += 1
        #try:
        ID = df.loc[i][0].replace(' ', '')
        data = dt.datetime.strptime(df.loc[i][2], '%d-%m-%Y %H:%M')
        requerente = '' if pd.isnull(df.loc[i][5]) else df.loc[i][5]
        descricao = df.loc[i][13].replace('\n','') 
        acompanhamento = '' if pd.isnull(df.loc[i][14]) else df.loc[i][14]
        nome = df.loc[i][1]
        munic = re.search(r'Município(.*?)UF', descricao).group(1) if re.search(r'Município(.*?)UF', descricao) != None else ''
        uf = re.search(r'UF(.*?)Logradouro', descricao).group(1) if re.search(r'UF(.*?)Logradouro', descricao) != None else ''
        log = re.search(r'Logradouro(.*?)Número', descricao).group(1) if re.search(r'Logradouro(.*?)Número', descricao) != None else ''
        novoNum = re.search(r'Número(.*?)Complemento', descricao).group(1) if re.search(r'Número(.*?)Complemento', descricao) != None else ''
        logComp = re.search(r'Complemento(.*?)CEP', descricao).group(1) if re.search(r'Complemento(.*?)CEP', descricao) != None else ''
        cep = re.search(r'CEP(.*?)(Telefone| )', descricao).group(1) if re.search(r'CEP(.*?)(Telefone| )', descricao) != None else ''
        telefone = re.search(r'Telefone(.*?)E-mail', descricao).group(1) if re.search(r'Telefone(.*?)E-mail', descricao) != None else ''
        email = re.search(r'E-mail \(opcional\)(.*?)Dados complementares', descricao).group(1) if re.search(r'E-mail \(opcional\)(.*?)Dados complementares', descricao) != None else ''
        codEndereco = re.search(r'Código do endereço(.*?)Código', descricao).group(1) if re.search(r'Código do endereço(.*?)Código', descricao) != None else ''
        setor = re.search(r'setor censitário(.*?)Melhor', descricao).group(1).strip() if re.search(r'setor censitário(.*?)Melhor', descricao) != None else None
        horarioFdsPrimeiro = re.search(r'final de semana(.*?)Melhor', descricao).group(1) if re.search(r'final de semana(.*?)Melhor', descricao) != None else ''
        horarioSemPrimeiro = re.search(r'dias de semana(.*?)$', descricao).group(1) if re.search(r'dias de semana(.*?)$', descricao) != None else ''
        novoCep = cep.replace('-','')
        novoCep = novoCep.replace('.','')
        novoCep = novoCep.replace(' ','')
        horarioFds = horarioFdsPrimeiro.replace(':00', 'h')
        horarioSem = horarioSemPrimeiro.replace(':00', 'h')
        horarioFds = horarioFds.replace(':30', 'h30')
        horarioSem = horarioSem.replace(':30', 'h30')
        horarioFds = horarioFds.replace(':oo', 'h')
        horarioSem = horarioSem.replace(':oo', 'h')
        logNum = re.search(r'([0-9]+)', novoNum).group(1) if re.search(r'([0-9]+)', novoNum) != None else '0'



        dict_dados = {'ID': ID, 'data': data, 'requerente': requerente, 'nome': nome, 'municipio': munic, 'logradouro': log,
                     'numero': logNum, 'complemento': logComp, 'cep':novoCep, 'telefone': telefone, 'email': email, 
                      'codEndereco': codEndereco, 'setor': setor, 'horario_fds': horarioFds, 'horario_sem': horarioSem, 'acompanhamento': acompanhamento}

        #Gerando o DataFrame dos chamados
        dfChamados = concatenar_dataframe_chamados(dfChamados, dict_dados)
        #print(f'novoCep {novoCep} logNum {logNum} municipio {munic} log {log} setor {setor}')

        #print(buscar_dados_id(cep, logNum, setor)['nom_comp_elem2'])

        # Gerando o DataFrame com os dados da consulta a base do IBGE
        if(isRec[0] == 'sim'): # sem sim para reuisição executar a funcação abaixo
            dfIbge = concatenar_dataframe_ibge(dfIbge, buscar_dados_id(novoCep, logNum, munic, log, setor), ID)




        if(exportarComo == 'xlsx'):
            worksheet.write(f'A{linha}', data) # Data
            worksheet.write(f'B{linha}', requerente) # Requerente
            worksheet.write(f'C{linha}', ID) # ID
            worksheet.write(f'D{linha}', nome) # Nome
            worksheet.write(f'E{linha}', munic) # municipio
            worksheet.write(f'F{linha}', f'{log}\nNúmero {logNum}\n{logComp}\nCEP {cep}') # Logradouro + Numero + complemento + cep
            worksheet.write(f'G{linha}', telefone) # Telefone
            worksheet.write(f'H{linha}', email) # Email
            worksheet.write(f'I{linha}', codEndereco) # Codigo de endereço 
            worksheet.write(f'J{linha}', setor) # setor
            worksheet.write(f'K{linha}', acompanhamento) # Acompanhamento - Descrição
            worksheet.write(f'L{linha}', horarioFds) # Final de Semana
            worksheet.write(f'M{linha}', horarioSem) # Durante a semana
            #worksheet.write(f'N{linha}', codIBGE) # Durante a semana

        
        print(f'fim de semana {horarioFds} semana {horarioSem}')
        if(i%10 == 0):
            print(f'Executado {i}x')
        certos += 1




        #erros +=1



    print(f'Concluido. Inserções: {certos}')
    workbook.close()
    dataframes_to_file(isRec[0], isRec[1], dfChamados, dfIbge)
    cruzar_dados(isRec[0], isRec[1])


# --------------- Executar as transformações de dataframe --------------



# In[16]:


def isRec():
    req = input("Deseja fazer a requisição no servidor? digite sim, caso deseje apenas tratar os dados digite qualquer coisa ")
    while((req != 'sim') or (req != 'nao')):
        req = input("Deseja fazer a requisição no servidor? digite sim, caso deseje apenas tratar os dados digite qualquer coisa ")
        if(req == 'sim' or req == 'nao'):
            break
    ext = input("Deseja extrair como xlsx ou csv?")
    while((ext != 'csv') or (ext != 'xlsx')):
        ext = input("Deseja extrair como xlsx ou csv?")
        if(ext == 'csv' or ext == 'xlsx'):    
            break
    
    return [req, ext]
        
        
def main():
    print("Antes de executar o script, certifique-se de que o arquivo 'glpi.csv' está no mesmo diretório deste script")
    funcao_Principal(isRec())
    

    


