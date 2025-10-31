## Consulta Lazy de Operações

> Redução de MIPS no Mainframe por meio de roteamento inteligente, cache e fallback seletivo.

### 1) Visão Executiva

- **Objetivo**: diminuir custos de MIPS no Mainframe priorizando uma camada otimizada (Lazy) quando elegível.
- **Rota alvo**: `/emprestimos/tuctuuc/operacoes`.
- **Abordagem**: três estratégias de consulta, regras de elegibilidade via Portal Manager, caching no Redis e fallback seletivo ao Mainframe.
- **Benefício**: respostas mais rápidas e menor consumo de Mainframe, sem perder cobertura funcional.

### 2) Escopo e Estratégias

- **Estratégias disponíveis**:
  - `CONSULTA_OPERACAO_OTIMIZADA`
  - `CONSULTA_CLIENTE_OTIMIZADA`
  - `CONSULTA_CONTA_OTIMIZADA`
- **Observação (Conta)**: consulta por conta lê dados do CC e utiliza o número único do cliente.

### 3) Governança de Acesso (Portal Manager)

- **Toggle**
  - `toggle.consulta-otimizada-ativa`: liga/desliga global da consulta otimizada.
- **Config (Piloto por consumidor)**
  - `config.regras-piloto-consulta-otimizada`.
  - **Prioridade**: se existir `id_cliente = "ALL"` com `status = "ATIVO"`, esta configuração prevalece.
  - **Estratégias de piloto**: `RDG`, `ULTIMO_DIGITO_OPERACAO`, `DAC`.
  - **Exemplo de payload**:
```json
{
  "configuracoes": [
    {
      "id_cliente": "UUID",
      "estrategias": [
        { "nome": "RDG", "valor": "01,03,03" }
      ],
      "custodias_mainframe": ["SF", "F5", "NSF", "U2"],
      "custodias_dynamo": ["SF", "F5", "NSF", "U2"],
      "produtos_nao_elegiveis": [1, 2, 3, 4],
      "status": "ATIVO"
    }
  ]
}
```
- **Tipos de consulta (chaves de ativação)**
  - `TIPO_CONSULTA_OPERACAO`
  - `TIPO_CONSULTA_CLIENTE`
  - `TIPO_CONSULTA_CONTA`

### 4) Fluxo Padronizado (alto nível)

1. Validar toggle global e tipo de consulta.
2. Avaliar elegibilidade conforme piloto/estratégia.
3. Tentar consulta otimizada (via Load Balancer).
4. Se sem dados: verificar Redis e decidir 404/lista vazia ou fallback seletivo ao Mainframe.
5. Se com dados: aplicar regras de custódia/produto e exceção F5.
6. Consolidar resposta e, se necessário, complementar com Mainframe apenas no essencial.
7. Atualizar Dynamo/Redis para reduzir futuras idas ao Mainframe.

> Diretriz: sempre filtrar e consultar o Mainframe apenas para as custodias necessárias.

### 5) Regras por Tipo de Consulta

#### 5.1) Por Operação

- **Elegibilidade**: estratégia (ex.: dois últimos dígitos) + chaves de ativação.
- **Não elegível**: fallback para Mainframe (custodias necessárias).
- **Elegível**: consulta otimizada.
  - **Sem dados**:
    - Checar Redis: "operação não existe no Mainframe".
      - Se não existe: responder 404 (economia de MIPS).
      - Se existe: filtrar custodias, consultar Mainframe.
        - Se retornar: emitir comando de atualização da operação.
        - Se não retornar: marcar Redis como "sem dados".
  - **Com dados**: aplicar regras de piloto por operação:
    - Custódia liberada?
    - Produto liberado?
    - Exceção F5 (Redis com TTL até 00:00 do dia; se consulta > 06:00 e marcação 00:00–06:00 do mesmo dia, considerar não elegível e buscar Mainframe).
  - **Se aprovado**: retornar dados com custo 0 de MIPS.

#### 5.2) Por Cliente

- **Elegibilidade**: estratégia (ex.: `RDG` em CPF/CNPJ) + chaves de ativação.
- **Não elegível**: fallback para Mainframe (custodias necessárias).
- **Elegível**: consulta otimizada.
  - **Sem dados**:
    - Checar Redis: "cliente sem operações nas custodias necessárias".
      - Se marcado: retornar lista vazia (não usar 404).
      - Se não marcado: filtrar custodias, consultar Mainframe.
        - Se retornar dados: marcar no Redis custodias sem dados, atualizar operações (DynamoDB) e retornar.
        - Se não retornar: marcar Redis "sem dados" e retornar lista vazia.
  - **Com dados**:
    - Aplicar regras por operação (custódia, produto, F5 com janela 00:00–06:00 > 06:00).
    - Separar elegíveis e não elegíveis.
      - Se todas elegíveis: retornar lista otimizada (completude validada por Glue Job de Validação).
      - Se houver não elegíveis: remover da resposta e consultar Mainframe apenas para as pendentes; atualizar Redis/Dynamo conforme retorno.

#### 5.3) Por Conta

- Lê dados do CC, utiliza o número único do cliente e segue o mesmo padrão: elegibilidade, otimizada primeiro, Redis, e fallback seletivo ao Mainframe.

### 6) Políticas de Cache e Fallback

- **Redis**
  - Marca "não existe/sem dados" por operação ou por cliente+custódia.
  - Evita chamadas desnecessárias ao Mainframe.
  - Exceção F5: TTL até 00:00 do dia; atenção à janela 00:00–06:00 quando consulta ocorre após 06:00.
- **Fallback Seletivo**
  - Sempre direcionado apenas às custodias requeridas pelo cliente.
  - Após retorno do Mainframe, emitir comandos de atualização e ajustar Redis conforme presença/ausência de dados.

### 7) Observabilidade e Governança

- **Métricas-chave**
  - Taxa de acerto da consulta otimizada vs. fallback.
  - Redução de MIPS por tipo de consulta e por custódia.
  - Latência P95/P99 por estratégia.
  - Taxa de 404/lisa vazia evitados via Redis.
- **Alertas/SLOs**
  - Erro na otimizada acima do limiar.
  - Aumento anômalo de fallbacks ou latência.

### 8) Riscos e Salvaguardas

- Configuração incorreta de piloto (ex.: `ALL` ativo indevidamente) pode ampliar escopo; mitigado por prioridade e validação de chaves.
- Janela F5 00:00–06:00: garantir consistência da marcação vs. horário da consulta.
- Coerência entre `custodias_dynamo` e `custodias_mainframe` para evitar lacunas de dados.

### 9) Anexo — Referência Rápida

- **Toggles**: `toggle.consulta-otimizada-ativa`
- **Configs**: `config.regras-piloto-consulta-otimizada`
- **Tipos de consulta**: `TIPO_CONSULTA_OPERACAO`, `TIPO_CONSULTA_CLIENTE`, `TIPO_CONSULTA_CONTA`
- **Estratégias de piloto**: `RDG`, `ULTIMO_DIGITO_OPERACAO`, `DAC`
- **Regra F5**: Redis TTL até 00:00; se consulta > 06:00 e marcação 00:00–06:00 do mesmo dia, considerar não elegível.
