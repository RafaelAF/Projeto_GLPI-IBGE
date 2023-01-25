# Projeto_GLPI-IBGE-Wilson

## Script para tratamento de dados de chamados disk censo.


`Disclaimer:` 
***Nenhum dado sensivel/pessoal esta sendo divulgado***, o intuito deste repositorio é me ajudar com versionamento e 
treinar programação em python ;p 
- (Wilson é um trocadilho por causa do filme 'Náufrago' ksks e essa demanda veio no final do censo com uma certa urgência e estavamos perdidos XD)

`Uma breve introdução:`
> Durante a reta final do censo surgiu o disk censo para população ligar para o IBGE para informar que não foi recenseado... enfim.
Apartir dai o volume da chamados chegou a um numero bem alto.

`Insight:`
> Devido a esse volume de chamados chegar a certo ponto e as informações contidas nele serem concatenadas em um texto so ao extrair relatorios, 
surgiu a ideia de automatizar esse processo de varrer os dados e retornar os dados tratados e validados (se os endereços passados eram de fato passados corretamente).

`Execução`
> Caso tente clonar esse script o mesmo não irá funcionar pois ele depende do arquivo 'glpi.csv' extraido dos chamados de um grupo daqual nem eu tenho acesso,
apenas meu chefe me passa, alem disso o script faz requisições a um webservice em rede interna. 
(mais uma vez resaltando que este repositorio tem apenas a intenção de documentar esta atividade)

`modos de excução`
\n O script tem 2 modos de input e 2 modos de output
- Fazer tratamento de dados e fazer requisições ao webservice 
- gerar o resultado em 'csv' ou 'xlsx'

> o codigo esta incompleto e esse readme tambem XD, mas vou atualizando ;p
