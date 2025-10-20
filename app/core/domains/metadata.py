import json

class Metadata:

    def __init__(self, trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.codigo_identificacao_carga = codigo_identificacao_carga
        self.quantidade_parcelas = int(trancode[0:3])
        self.quantidade_movimentos_financeiros = int(trancode[3:6])
        self.quantidade_parcelas_recebidas = self.__recuperar_dados_parcelas(dados_todos_dominios)

    def to_json(self):
        return json.dumps(self.__dict__)

    def __recuperar_dados_parcelas(self, dados_todos_dominios):
        lista_map = list(map(lambda x: json.loads(x), dados_todos_dominios))
        lista =  list(filter(lambda x: x['dominio'] == 'PARCELA', lista_map))
        if(len(lista) > 0):
            parcela_dto = lista[0]
            return len(parcela_dto['lista_trancode'])
        return 0



<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <title>Antes vs Depois — Visão Executiva</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root{
      --orange:#ff7a00; --orange-200:#ffefe1; --orange-100:#fff7ef;
      --text:#1b1b1b; --muted:#6b7280; --bg:#ffffff; --line:#f3f4f6;
      --card:#ffffff; --shadow:0 8px 24px rgba(0,0,0,.06);
      --green:#10b981; --green-100:#d1fae5; --green-200:#a7f3d0;
      --red:#ef4444; --red-100:#fee2e2; --red-200:#fecaca;
      --blue:#3b82f6; --blue-100:#dbeafe; --blue-200:#bfdbfe;
    }
    *{box-sizing:border-box}
    html,body{height:100%}
    body{
      margin:0; background:var(--bg); color:var(--text);
      font:16px/1.55 "Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      -webkit-font-smoothing:antialiased; -moz-osx-font-smoothing:grayscale;
    }
    .wrap{max-width:1080px; margin:0 auto; padding:28px 20px}
    header{display:flex; align-items:flex-start; justify-content:space-between; gap:16px; margin-bottom:20px}
    h1{margin:0; font-size:26px; letter-spacing:.2px}
    .subtitle{margin:6px 0 0; color:var(--muted)}
    .brand{color:var(--orange); font-weight:700; letter-spacing:.4px}

    .section{
      background:var(--card); border:1px solid var(--line); border-radius:14px; padding:18px 18px 16px; margin:16px 0; box-shadow:var(--shadow)
    }
    .section h2{margin:6px 0 10px; font-size:18px; letter-spacing:.2px}
    .tag{
      display:inline-flex; align-items:center; gap:8px;
      font-size:12px; color:#8a4600; background:var(--orange-100);
      padding:6px 10px; border:1px solid var(--orange-200); border-radius:999px; font-weight:600;
    }
    .tag--success{color:#065f46; background:var(--green-100); border-color:var(--green-200)}
    .tag--warning{color:#92400e; background:#fef3c7; border-color:#fde68a}
    .desc{color:var(--muted); margin:6px 0 12px}

    .grid{display:grid; gap:14px; grid-template-columns:1fr}
    @media(min-width:860px){ .grid{grid-template-columns:1fr 1fr} }

    .box{
      background:#fff; border:1px solid var(--line); border-radius:12px; padding:14px 14px 12px;
    }
    .box h3{margin:0 0 8px; font-size:14px; color:var(--muted); font-weight:700; letter-spacing:.3px}
    ul{margin:8px 0; padding-left:18px}
    ul li{margin:6px 0}

    .cards3{display:grid; gap:12px; grid-template-columns:1fr}
    @media(min-width:920px){ .cards3{grid-template-columns:1fr 1fr 1fr} }
    .card{
      background:#fff; border:1px solid var(--line); border-radius:12px; padding:14px 14px 12px;
    }
    .card--warn{background:var(--orange-100); border-color:var(--orange-200)}
    .card--success{background:var(--green-100); border-color:var(--green-200)}
    .card__title{display:flex; align-items:center; gap:8px; font-weight:700; color:var(--orange); margin-bottom:8px}
    .card--success .card__title{color:var(--green)}
    .dot{width:8px; height:8px; border-radius:50%; background:var(--orange); display:inline-block}
    .card--success .dot{background:var(--green)}

    .flow{
      display:flex; align-items:center; gap:8px; flex-wrap:wrap;
      background:#fff; border:1px dashed var(--line); border-radius:12px; padding:10px 12px; margin-top:10px
    }
    .pill{
      border:1px solid var(--line); border-radius:999px; padding:6px 12px; background:#fff; font-weight:600
    }
    .arrow{color:var(--orange); font-weight:800}

    .table{
      margin-top:12px; border:1px solid var(--line); border-radius:12px; overflow:hidden;
      background:#fff
    }
    .row{display:grid; grid-template-columns:1fr 1fr}
    .head{background:var(--orange-100); font-weight:700}
    .cell{padding:10px 12px; border-bottom:1px solid var(--line)}
    .row:last-child .cell{border-bottom:0}

    /* Nova tabela de consumo de APIs */
    .consumption-table{
      margin-top:16px; border:1px solid var(--line); border-radius:12px; overflow:hidden;
      background:#fff; box-shadow:0 2px 8px rgba(0,0,0,.04)
    }
    .consumption-table .row{
      display:grid; grid-template-columns:120px 1fr 100px 100px 100px 120px;
      align-items:center;
    }
    .consumption-table .head{
      background:var(--blue-100); font-weight:700; font-size:13px; text-transform:uppercase; letter-spacing:.5px
    }
    .consumption-table .cell{
      padding:12px 14px; border-bottom:1px solid var(--line); font-size:14px
    }
    .consumption-table .row:last-child .cell{border-bottom:0}
    .consumption-table .row:nth-child(even){background:#fafafa}
    .consumption-table .row:hover{background:var(--blue-50)}

    .api-id{font-family:monospace; font-size:12px; color:var(--blue); font-weight:600}
    .percentage{font-weight:700; text-align:center}
    .percentage--high{color:var(--red)}
    .percentage--medium{color:var(--orange)}
    .percentage--low{color:var(--green)}
    .cost{font-weight:600; color:var(--text)}
    .savings{font-weight:700; color:var(--green)}

    .progress-bar{
      width:100%; height:8px; background:var(--line); border-radius:4px; overflow:hidden; margin:4px 0
    }
    .progress-fill{
      height:100%; background:linear-gradient(90deg, var(--red), var(--orange), var(--green));
      transition:width 0.3s ease;
    }

    .economy-cards{
      display:grid; gap:12px; grid-template-columns:1fr; margin-top:16px
    }
    @media(min-width:768px){ .economy-cards{grid-template-columns:1fr 1fr 1fr} }
    .economy-card{
      background:#fff; border:1px solid var(--line); border-radius:12px; padding:16px; text-align:center
    }
    .economy-card--total{background:var(--green-100); border-color:var(--green-200)}
    .economy-card__value{
      font-size:24px; font-weight:800; margin:8px 0 4px
    }
    .economy-card--total .economy-card__value{color:var(--green)}
    .economy-card__label{
      font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px
    }

    .kicker{color:var(--muted); font-size:13px}
    .highlight{color:var(--orange); font-weight:700}

    .benefits li{margin:6px 0}

    @media print{
      .section{box-shadow:none}
      .wrap{padding:0}
      .tag{border-color:#ffddb8}
    }
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>Antes vs Depois <span class="brand">| Dados & Operações</span></h1>
        <p class="subtitle">Resumo executivo de problemas, impactos e soluções adotadas.</p>
      </div>
    </header>

    <!-- Consulta Lazy (profissional/visual) -->
    <div class="section">
      <span class="tag">Consulta de Operações (Lazy)</span>
      <h2>Como funcionava e por que desligamos</h2>
      <p class="kicker">Fluxo anterior com fallback e impactos na integridade.</p>

      <div class="cards3">
        <div class="card">
          <div class="card__title"><span class="dot"></span>Fluxo (antes)</div>
          <ul>
            <li>Consulta primária ao <strong>DynamoDB</strong>.</li>
            <li>Se não encontrado, <strong>fallback no Mainframe</strong>.</li>
          </ul>
        </div>

        <div class="card card--warn">
          <div class="card__title"><span class="dot"></span>Problema</div>
          <ul>
            <li><strong class="highlight">Integridade da consulta comprometida</strong>.</li>
            <li>Inconsistências propagadas a outros fluxos.</li>
          </ul>
        </div>

        <div class="card">
          <div class="card__title"><span class="dot"></span>Decisão</div>
          <ul>
            <li><strong>Desligamento do modo Lazy</strong> para estancar inconsistências.</li>
            <li>Foco nos Serviços de Negócio POS‑VENDA (Cancelamento, Amortizações, Estorno, Front‑End, etc.).</li>
          </ul>
        </div>
      </div>

      <div class="flow" aria-label="Fluxo anterior">
        <div class="pill">Cliente</div>
        <div class="arrow">→</div>
        <div class="pill">DynamoDB</div>
        <div class="arrow">→</div>
        <div class="pill">Mainframe (fallback)</div>
      </div>

      <div class="box" style="margin-top:12px">
        <h3>Aprendizados</h3>
        <ul>
          <li>Faltou <strong class="highlight">piloto e acompanhamento</strong> definidos desde o início.</li>
          <li>Desligamos para evitar dividir esforços entre correção da consulta e estabilidade dos demais domínios.</li>
        </ul>
      </div>
    </div>

    <!-- 1) Atomicidade DynamoDB -->
    <div class="section">
      <span class="tag">Problema 1</span>
      <h2>Atomicidade no DynamoDB</h2>
      <p class="desc">Dados parciais geravam <span class="highlight">saldo devedor inconsistente</span>.</p>

      <div class="grid">
        <div class="box">
          <h3>Antes</h3>
          <ul>
            <li>Múltiplos itens por operação (parciais).</li>
            <li>Escritas não atômicas.</li>
          </ul>
        </div>
        <div class="box">
          <h3>Depois</h3>
          <ul>
            <li>Único item por operação (todas as ocorrências agregadas).</li>
            <li>Compressão para manter &lt; 400KB.</li>
          </ul>
        </div>
      </div>

      <div class="table">
        <div class="row head">
          <div class="cell">DE</div>
          <div class="cell">PARA</div>
        </div>
        <div class="row">
          <div class="cell">Múltiplos itens parciais</div>
          <div class="cell">Registro único por operação</div>
        </div>
        <div class="row">
          <div class="cell">Inconsistência de saldo</div>
          <div class="cell">Escrita atômica e leitura consistente</div>
        </div>
      </div>
    </div>

    <!-- 2) Batch: Lambda -> Glue -->
    <div class="section">
      <span class="tag">Problema 2</span>
      <h2>Cargas offline em Lambdas</h2>
      <p class="desc">Timeouts de 15 min e <span class="highlight">alto paralelismo</span> afetavam a conta.</p>

      <div class="grid">
        <div class="box">
          <h3>Antes</h3>
          <ul>
            <li>ETL em Lambda com encadeamentos.</li>
            <li>Impacto em outros fluxos.</li>
          </ul>
        </div>
        <div class="box">
          <h3>Depois</h3>
          <ul>
            <li>Job dedicado no AWS Glue (Spark).</li>
            <li>Logs, métricas e checkpoints.</li>
          </ul>
        </div>
      </div>

      <div class="table">
        <div class="row head">
          <div class="cell">DE</div>
          <div class="cell">PARA</div>
        </div>
        <div class="row">
          <div class="cell">Timeouts e encadeamentos</div>
          <div class="cell">Execução escalável e resiliente</div>
        </div>
        <div class="row">
          <div class="cell">Concorrência descontrolada</div>
          <div class="cell">Orquestração e janelas definidas</div>
        </div>
      </div>
    </div>

    <!-- 3) Domínios separados -> Contrato único -->
    <div class="section">
      <span class="tag">Problema 3</span>
      <h2>Domínios e buckets separados</h2>
      <p class="desc">Chegadas desencontradas (5–15 min) e <span class="highlight">quebra de integridade</span>.</p>

      <div class="grid">
        <div class="box">
          <h3>Antes</h3>
          <ul>
            <li>Arquivos e pipelines por domínio.</li>
            <li>Eventos do mesmo negócio em tempos distintos.</li>
          </ul>
        </div>
        <div class="box">
          <h3>Depois</h3>
          <ul>
            <li>Contrato único por operação.</li>
            <li>Arquivo único até 5GB.</li>
          </ul>
        </div>
      </div>

      <div class="table">
        <div class="row head">
          <div class="cell">DE</div>
          <div class="cell">PARA</div>
        </div>
        <div class="row">
          <div class="cell">Pipelines por domínio</div>
          <div class="cell">Entrega coesa em um único arquivo</div>
        </div>
        <div class="row">
          <div class="cell">Sincronização difícil</div>
          <div class="cell">Consumo simples e determinístico</div>
        </div>
      </div>
    </div>

    <!-- Nova seção: Análise de Consumo de APIs -->
    <div class="section">
      <span class="tag tag--warning">Análise de Consumo</span>
      <h2>Consumo de Recursos por Cliente</h2>
      <p class="desc">Identificação dos maiores consumidores de recursos e <span class="highlight">oportunidades de otimização</span>.</p>

      <div class="consumption-table">
        <div class="row head">
          <div class="cell">API ID</div>
          <div class="cell">Cliente</div>
          <div class="cell">Consumo</div>
          <div class="cell">Custo/Mês</div>
          <div class="cell">Economia</div>
          <div class="cell">Status</div>
        </div>
        <div class="row">
          <div class="cell"><span class="api-id">API-001</span></div>
          <div class="cell">Cliente A (E-commerce)</div>
          <div class="cell">
            <div class="percentage percentage--high">42.3%</div>
            <div class="progress-bar">
              <div class="progress-fill" style="width: 42.3%"></div>
            </div>
          </div>
          <div class="cell"><span class="cost">R$ 8.450</span></div>
          <div class="cell"><span class="savings">R$ 2.535</span></div>
          <div class="cell"><span class="tag tag--warning">Crítico</span></div>
        </div>
        <div class="row">
          <div class="cell"><span class="api-id">API-002</span></div>
          <div class="cell">Cliente B (Fintech)</div>
          <div class="cell">
            <div class="percentage percentage--high">28.7%</div>
            <div class="progress-bar">
              <div class="progress-fill" style="width: 28.7%"></div>
            </div>
          </div>
          <div class="cell"><span class="cost">R$ 5.740</span></div>
          <div class="cell"><span class="savings">R$ 1.722</span></div>
          <div class="cell"><span class="tag tag--warning">Alto</span></div>
        </div>
        <div class="row">
          <div class="cell"><span class="api-id">API-003</span></div>
          <div class="cell">Cliente C (SaaS)</div>
          <div class="cell">
            <div class="percentage percentage--medium">15.2%</div>
            <div class="progress-bar">
              <div class="progress-fill" style="width: 15.2%"></div>
            </div>
          </div>
          <div class="cell"><span class="cost">R$ 3.040</span></div>
          <div class="cell"><span class="savings">R$ 912</span></div>
          <div class="cell"><span class="tag">Médio</span></div>
        </div>
        <div class="row">
          <div class="cell"><span class="api-id">API-004</span></div>
          <div class="cell">Cliente D (Marketplace)</div>
          <div class="cell">
            <div class="percentage percentage--medium">8.9%</div>
            <div class="progress-bar">
              <div class="progress-fill" style="width: 8.9%"></div>
            </div>
          </div>
          <div class="cell"><span class="cost">R$ 1.780</span></div>
          <div class="cell"><span class="savings">R$ 534</span></div>
          <div class="cell"><span class="tag">Baixo</span></div>
        </div>
        <div class="row">
          <div class="cell"><span class="api-id">API-005</span></div>
          <div class="cell">Cliente E (Startup)</div>
          <div class="cell">
            <div class="percentage percentage--low">4.9%</div>
            <div class="progress-bar">
              <div class="progress-fill" style="width: 4.9%"></div>
            </div>
          </div>
          <div class="cell"><span class="cost">R$ 980</span></div>
          <div class="cell"><span class="savings">R$ 294</span></div>
          <div class="cell"><span class="tag tag--success">Otimizado</span></div>
        </div>
      </div>

      <!-- Cards de Projeção de Economia -->
      <div class="economy-cards">
        <div class="economy-card">
          <div class="economy-card__label">Economia Total</div>
          <div class="economy-card__value">R$ 5.997</div>
          <div class="economy-card__label">por mês</div>
        </div>
        <div class="economy-card">
          <div class="economy-card__label">Economia Anual</div>
          <div class="economy-card__value">R$ 71.964</div>
          <div class="economy-card__label">projeção</div>
        </div>
        <div class="economy-card economy-card--total">
          <div class="economy-card__label">Redução de Custo</div>
          <div class="economy-card__value">30%</div>
          <div class="economy-card__label">média geral</div>
        </div>
      </div>

      <div class="box" style="margin-top:16px">
        <h3>Recomendações de Otimização</h3>
        <ul>
          <li><strong>Cliente A (API-001):</strong> Implementar cache Redis para reduzir 60% das consultas ao DynamoDB.</li>
          <li><strong>Cliente B (API-002):</strong> Otimizar queries com índices compostos e paginação eficiente.</li>
          <li><strong>Clientes C-D:</strong> Implementar rate limiting e throttling para controlar picos de uso.</li>
          <li><strong>Monitoramento:</strong> Alertas automáticos quando consumo exceder 25% do total.</li>
        </ul>
      </div>
    </div>

    <!-- Benefícios -->
    <div class="section">
      <span class="tag tag--success">Benefícios</span>
      <h2>Resultados consolidados</h2>
      <ul class="benefits">
        <li>Consistência transacional no DynamoDB (agregação + compressão).</li>
        <li>Batch estável e rápido com Glue (sem timeouts de Lambda).</li>
        <li>Integridade temporal com contrato único por operação.</li>
        <li>Simplicidade operacional e redução de retrabalho.</li>
        <li><strong class="highlight">Economia de R$ 72k/ano</strong> com otimização de consumo de APIs.</li>
      </ul>
    </div>
  </div>
</body>
</html>

