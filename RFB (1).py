# Databricks notebook source
# Esse ntoebook está configurado para realizar o download de todos os arquivos do RFB quando a data de atualização do mesmo for maior que a do blob.

# Instruções de download
# Caso queria baixar manualmente é necessário colocar o valor "1" no parâmetro "Exec_manual"

# Parâmetro Arquivo
# Nele é onde podemos especificar qual arquivo baixar, como '*' para todos ou digitando um ou mais nomes (Divididos por vírgula) para especificar um arquivo, como: Estabelecimentos, socios, empresas

#Parãmetro tabela
# Em fase de implantação, como cada tema é dividido em 10 arquivos, a ideia é poder escolher qual arquivo específico baixar.

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC pip install bs4

# COMMAND ----------

import requests
import urllib
import os
from bs4 import BeautifulSoup
from datetime import datetime
import json

# COMMAND ----------

table = dbutils.widgets.get("Tabela")
file = dbutils.widgets.get("Arquivo")
exe_manual = dbutils.widgets.get("Exec_manual")
#Define a lista de dimensões para download
rfb_dims = {
    ('PAISCSV', 'DIM_RFB_PAIS'): '',
    ('SIMPLES', 'DIM_RFB_SIMPLES_NACIONAL'): '',
    ('CNAECSV', 'DIM_RFB_ATRIBUTOS_CNAE'): '',
    ('MOTICSV', 'DIM_RFB_MOTIVO_SITUACAO'): '',
    ('MUNICCSV', 'DIM_RFB_MUNICIPIOS'): '',
    ('NATJUCSV', 'DIM_RFB_NATU_JUR'): '',
    ('QUALSCSV', 'DIM_RFB_QUALI_SOCIOS'): ''
    
    }
#Define o caminho do download
save_path = '/dbfs/mnt/bigdatadev/landing/RFB/'

# COMMAND ----------

# !pip install tqdm

# COMMAND ----------

# from tqdm import tqdm

def download_url(url, save_path, chunk_size=128):
    
#     response = requests.get(url, stream=True)
    response = urllib.request.urlopen(url)

    total_size_in_bytes = int(response.headers.get('content-length', 0))
    kib : float = total_size_in_bytes/1024
    mib : float = kib  * 0.000976563
    
    if mib > 1:
        print(f'Tamanho do arquivo: {mib:.2f} MiB.')
    else:
        print(f'Tamanho do arquivo: {kib:.2f} KiB.')

#     block_size = 1024 #1 Kibibyte
#     progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, maxinterval=5)
    
    with open(save_path, 'wb') as f:
        f.write(bytes(response.read()))
#     with open(save_path, 'wb') as f:
#         for data in response.iter_content(block_size):
#             progress_bar.update(len(data))
# #             print(len(data))
#             f.write(data)
#     progress_bar.close()

# COMMAND ----------

r = requests.get("https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj")

rfb_empresas = []
rfb_estab = []
rfb_socios = []

soup = BeautifulSoup(r.text,features="html.parser")

# COMMAND ----------

for a in soup.find_all('div',class_=False,temprop=None):
    find = a.text
    if find.split(':')[0] == 'Data da última extração':
        rfb_modificacao = find.split(':')[1].strip()
        print("RFB - Data da última extração:", rfb_modificacao)

# COMMAND ----------

#Reseta lista para caso esse cmd seja executado mais de 1 vez
rfb_empresas = []
rfb_estab = []
rfb_socios = []
list_return_variable = []

list_files = file.upper().split(',')
print(save_path)
fdpaths = [save_path+"/RAW/"+fd+'/' for fd in os.listdir(save_path) if ('RAW' not in fd) and ('Simples' not in fd)]
print(fdpaths)


for a in soup.find_all('a',{"class": "external-link"},href=True):
#     print(tipo)
    tipo = a['href'].split('.')
    if "EMPRE" in str(tipo):
        rfb_empresas.append(a['href'])
    elif "ESTABE" in str(tipo):
        rfb_estab.append(a['href']) 
    elif "SOCIO" in str(tipo):
        rfb_socios.append(a['href']) 
    else:
        for i in list(rfb_dims):
            if i[0] in str(a['href']):
                rfb_dims.update({(i[0],i[1]):a['href']})

try:
    for fdpath in fdpaths:
        print(fdpath)
        nm_pasta = fdpath.split('/')[-2]
        statinfo = os.stat(fdpath)
        create_date = datetime.fromtimestamp(statinfo.st_ctime)
        modified_date = datetime.fromtimestamp(statinfo.st_mtime)
        dl_modificacao = modified_date.strftime("%d/%m/%Y")

        if (rfb_modificacao > dl_modificacao) or (exe_manual == '1'):
            list_return_variable.append(1)
            for a in list_files:
                if nm_pasta == 'Dimensoes':    
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print("#                                                   Dimensões                                                  #")
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print(f'# -> Quantidade de Arquivos: {len(rfb_dims)}.                                                                                #')
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    for dim in rfb_dims:
                        if a.strip() == '*':
                            print(f'\nBaixando --  {dim[1]} -- Link: {rfb_dims[dim]}')
                            print('Salvando em: ' + save_path+f'RAW/Dimensoes/{dim[1]}.zip')
                            download_url(rfb_dims[dim],save_path+f'RAW/Dimensoes/{dim[1]}.zip')
                            print(f'--- Download da dimensão {dim[1]} concluido. ---')
                        elif a.strip() in dim[1]:
                            print(f'\nBaixando --  {dim[1]} -- Link: {rfb_dims[dim]}')
                            print('Salvando em: ' + save_path+f'RAW/Dimensoes/{dim[1]}.zip')
                            download_url(rfb_dims[dim],save_path+f'RAW/Dimensoes/{dim[1]}.zip')
                            print(f'--- Download da dimensão {dim[1]} concluido. ---\n')

                elif nm_pasta == 'Empresas':
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print("#                                                    Empresas                                                  #")
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print(f'# -> Quantidade de Arquivos: {len(rfb_empresas)}.                                                                               #')
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    index = 0
                    for chunk_empresa in rfb_empresas:
                        print(f'------- {index+1}º Arquivo.', end='')
                        if (a.strip() == '*') or (a.strip() in nm_pasta.upper()):
                            name_empresas = f'RFB_EMPRESAS_{index}.zip'
                            print(f'\nBaixando -- ' + name_empresas + ' -- Link: ' + chunk_empresa)
                            print('Salvando em: ' + save_path+f'RAW/Empresas/{name_empresas}')
                            download_url(chunk_empresa,save_path+f'RAW/Empresas/{name_empresas}')
                            print(f'--- Download da dimensão {name_empresas} concluido. ---\n')
                            print('-----------------------------------------')
                            index += 1

                elif nm_pasta == 'Estabelecimento':
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print("#                                                Estabelecimento                                               #")
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print(f'# -> Quantidade de Arquivos: {len(rfb_estab)}.                                                                               #')
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    index = 0
                    for chunk_estab in rfb_estab:
                        print(f'------- {index+1}º Arquivo.', end='')
                        if (a.strip() == '*') or (a.strip() in nm_pasta.upper()):
                            name_estab = f'RFB_ESTAB_{index}.zip'
                            print(f'\nBaixando -- ' + name_estab + ' -- Link: ' + chunk_estab)
                            print('Salvando em: ' + save_path+f'RAW/Estabelecimento/{name_estab}')
                            download_url(chunk_estab,save_path+f'RAW/Estabelecimento/{name_estab}')
                            print(f'--- Download da dimensão {name_estab} concluido. ---\n')
                            index += 1
                elif nm_pasta == 'Socios':
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print("#                                                    Socios                                                    #")
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    print(f'# -> Quantidade de Arquivos: {len(rfb_socios)}.                                                                                #')
                    print('# ------------------------------------------------------------------------------------------------------------ #')
                    index = 0
                    for chunk_socios in rfb_socios:
                        print(save_path)
                        print(f'------- {index+1}º Arquivo.', end='')
                        if (a.strip() == '*') or (a.strip() in nm_pasta.upper()):
                            name_socios = f'RFB_SOCIOS_{index}.zip'
                            print(f'\nBaixando -- ' + name_socios + ' -- Link: ' + chunk_socios)
                            print('Salvando em: ' + save_path+f'RAW/Socios/{name_socios}')
                            download_url(chunk_socios,save_path+f'RAW/Socios/{name_socios}')
                            print(f'--- Download da dimensão {name_socios} concluido. ---')
                            index += 1
        else:
            list_return_variable.append(0)
            print("Não é necessário realizar carga para: ", nm_pasta,
                  "\nData de extração RFB: ", rfb_modificacao, 
                  "\nData de modificação no Data Lake: ", dl_modificacao)

except Exception as e:
    print(f'{type(e)}: {e}.')

# COMMAND ----------

if 1 in list_return_variable:
    dbutils.notebook.exit(json.dumps({'continue':'1'}))
else:
    dbutils.notebook.exit(json.dumps({'continue':'0'}))
