# Sistema P2P Escalável

Implementação de um sistema P2P escalável para processamento distribuído de tarefas.

## Funcionalidades

- **Service Discovery**: Peers descobrem automaticamente o Master via broadcast UDP
- **Registro e Heartbeat**: Peers se registram no Master e mantêm conexão ativa
- **Distribuição de Tarefas**: Master distribui tarefas (arquivos ZIP) para os Peers
- **Processamento**: Peers executam scripts Python em arquivos CSV
- **Submissão de Resultados**: Peers enviam resultados de volta para o Master

## Como Executar

### Pré-requisitos

- Python 3.8 ou superior

### Executando o Master

```bash
python master/master.py