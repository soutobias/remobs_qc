## REMOBS_QC

Essa pasta contem uma lista de códigos para processamento em tempo real dos dados das boias meteoceanográficas do Programa Nacional de Boias
e do projeto REMO Observacional.

Cada pasta representa os seguintes códigos:

- alerts: códigos para envio de alerta em caso das boias pararem de transmitir dados ou saírem de posição;
- boias: códigos para cada tipo de boia (Axys, BMO-BR, easywave e spotter). Em cada pasta de boia, á códigos para coleta de dados brutos em tempo real, acesso ao bando de dados e controle de qualidade de dados em tempo real;
- bufr: código para codificação em bufr dos dados meteorológicos e oceanográficos transmitidos pelas boias. Esta codificação em bufr é regida pela WMO e é usada para envio dados para o Global Telecomunication System
- netcdf: código para converter os dados das boias para o formato NETCDF, utilizando o CF Conventions.
- notebooks: notebooks criados para análises e testes com os códigos do projeto
- qc_checks: códigos de controle de qualidade de dados coletados pelas boias. Estes códigos foram elaborados utilizando como referência os manuais do QARTOD e do NDBC. Para maiores informações sobre estes manuais, acessar [MANUAL QC PNBOIA](https://www.marinha.mil.br/chm/sites/www.marinha.mil.br.chm/files/u1947/controle_de_qualidade_dos_dados.pdf)
- smm: códigos para transmissão dos dados das boias por email para os sistemas de distribuição de dados da Marinha do Brasil
- tags: códigos para obtenção dos dados transmitidos por transmissores de posição instalados nas boias.
