# O Desafio

O Tech Challenge é um desafio técnico, feito em grupo que, todo final de módulo é apresentado para que os alunos apliquem os conhecimentos adquiridos no decorrer das aulas.

## O problema

Do ponto de vista de "um grande hospital", entender como foi o comportamento da população no período da COVID-19 e quais indicadores seriam importante para o planejamento, caso haja um novo surto da doença.

Para a realização desta análise, será utilizado o estudo do PNAD-COVID19 do IBGE, como base para busca da resposta do problema proposto.

## Dados triviais

O desafio deixa o grupo livre para definir quais são os dados que serão utilizados na análise, contudo, três dados são triviais:

- Característica clínicas dos sintomas;
- Características da população;
- Características econônimcas da sociedade.

### Dados selecionados

**Dados básicos**
Dados básicos sobre respondentes, utilizado para contexto, segmentação e identificação da população pesquisada.

- **Código da Variável**: UF | **Descrição da Variável** Unidade da Federação
- **Código da Variável**: CAPITAL | **Descrição da variável**: Capital do Estado
- **Código da Variável**: RM_RIDE | **Descrição da variável**: Região Metropolitana Adminstrativa Integrada de Desenvolvimento
- **Código da Variável**: V1012 | **Descrição da variável**: Semana no mês - 1 a 4 (indicando qual semana em do mês em que a pesquisa foi tomada)
- **Código da Variável**: V1013 | **Descrição da variável**: Mês da pesquisa - 1 a 12 (indicando em qual mês do ano a pesquisa foi tomada)
- **Código da Variável**: V1022 | **Descrição da Variável**: Situação do domicílio - 1 | Urbanda; 2 | Rural
- **Código da Variável**: V1023 | **Descrição da Variável**: Tipo de área - 1 | Capital; 2 | Resto da RM (Região Metropolitana, excluindo a capital); 3 | Resto da RIDE (Região Integrada de Desenvolvimento Econômico, excluindo a capital); 4 | Resto da UF  (Unidade da Federação, excluindo a região metropolitana e a RIDE)
- **Código da Variável**: A002 | **Descrição da Variável**: 000 a 130 (idade em anos)
- **Código da Variável**: A003 | **Descrição da Variável**: Sexo (gênero) - 1 | Homem; 2 | Mulher
- **Código da Variável**: A004 | **Descrição da Variável**: Cor ou raça - 1 | Branca; 2 | Preta; 3 | Amarela; 4 | Parda; 5 | Indígena; 9 | Ignorado
- **Código da Variável**: A005 | **Descrição da Variável**: Escolaridade - 1 | Sem instrução; 2 | Fundamental incompleto; 3 | Fundamental completa; 4 | Médio Incompleto; 5 | Médio completo; 6 | Superio incompleto; 7 | Superior completo; 8 | Pós-graduação, mestrado ou doutorado

## Objetivo

Ao utilizar a base do [PNAD-COVID-19 do IBGE](https://covid19.ibge.gov.br/pnad-covid/), será criada uma nova base para análise, considerando as seguintes características:

- Utilização de no máximo 20 questionamentos realizados na pesquisa;
- Utilizar 3 meses para construção da solução;
- Caracterização dos sintomas clínicos da população;
- Comportamento da população na época da COVID-19;
- Características econômicas da Sociedade;

À partir disso, desenvolver uma breve análise sobre o processo de organização do banco, as perguntas selecionadas para a resposta do problema e quais seriam as principais ações que o hospital deverá tomar em caso de um novo surto de COVID-19.

