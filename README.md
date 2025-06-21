# Bot di Arbitraggio Triangolare per Binance

Questo progetto è un bot avanzato per il rilevamento di opportunità di arbitraggio triangolare sull'exchange di criptovalute Binance. Il bot si connette in tempo reale ai flussi di dati di mercato, calcola migliaia di potenziali percorsi di arbitraggio al secondo e implementa una serie di filtri strategici e tecnici per identificare solo le opportunità realistiche e potenzialmente eseguibili.

## Architettura e Design

Il bot è costruito su un'architettura ibrida ad alte prestazioni che sfrutta il meglio della programmazione concorrente in Python per massimizzare l'efficienza e la stabilità.

### 1. `asyncio` per l'I/O di Rete
Il thread principale del programma è gestito da `asyncio`. Questo gli permette di gestire in modo estremamente efficiente centinaia di operazioni di Input/Output simultaneamente, come:
- Mantenere aperte e ricevere dati da multiple connessioni WebSocket con Binance.
- Scrivere in modo non bloccante su diversi file di log.
- Inviare notifiche all'API di Telegram senza "congelare" il resto del programma.
- Gestire task periodici come il monitoraggio delle performance e i riepiloghi orari.

### 2. `ProcessPoolExecutor` per i Calcoli Intensivi
Il calcolo delle opportunità di arbitraggio è un'operazione che richiede un uso intensivo della CPU. Se fosse eseguita nel thread principale di `asyncio`, bloccherebbe l'intero programma, causando la disconnessione dai WebSocket. Per evitare ciò, questi calcoli vengono delegati a un pool di processi separati. Questo permette al bot di sfruttare tutti i core della CPU per i calcoli pesanti, mentre il thread principale rimane libero e reattivo per gestire la rete.

### 3. Approccio a Grafo per l'Efficienza
A differenza di un approccio a forza bruta che controlla ogni possibile combinazione di tre valute, questo bot implementa una logica molto più intelligente basata sulla **teoria dei grafi**:
1.  **Costruzione del Grafo:** All'inizio di ogni ciclo di analisi, il bot costruisce una "mappa" delle connessioni dirette, dove ogni valuta è un nodo e ogni coppia di trading esistente è un arco.
2.  **Navigazione Efficiente:** I processi worker non generano più permutazioni casuali. Invece, navigano il grafo pre-calcolato, esplorando **solo ed esclusivamente percorsi di trading a 3 passi che esistono realmente**.

Questo approccio elimina milioni di calcoli inutili per ogni ciclo, riducendo drasticamente il numero di "percorsi non validi" e concentrando la potenza della CPU solo sull'analisi di opportunità concrete.

## Funzionalità e Filtri

L'efficacia del bot risiede nella sua capacità di scartare il "rumore" di mercato. Per questo, sono stati implementati diversi livelli di filtro.

### Filtri Tecnici di Simulazione
Ogni percorso valido viene sottoposto a una simulazione realistica che deve superare tutti i seguenti controlli per ogni "gamba" del triangolo:
- **Liquidità:** La quantità richiesta per il trade deve essere disponibile sull'order book.
- **`minQty`:** La quantità scambiata deve essere superiore alla soglia minima richiesta da Binance per quella coppia.
- **`minNotional`:** Il valore totale del trade (quantità x prezzo) deve superare il valore nozionale minimo richiesto (solitamente intorno ai 10$).
- **`stepSize`:** La quantità scambiata viene arrotondata per difetto per rispettare la precisione decimale richiesta da Binance per quella coppia. Se la quantità arrotondata è zero, il trade viene scartato.

### Filtri Strategici
- **Asset di Partenza Prioritari:** Per massimizzare la praticità e concentrare l'analisi, la strategia principale considera solo i percorsi di arbitraggio che iniziano e finiscono con uno degli asset definiti come prioritari (es. `USDT`, `BTC`, `ETH`, `SOL`, ecc.).
- **Cooldown delle Opportunità:** Per evitare notifiche ripetute per la stessa opportunità volatile, una volta che un triangolo viene notificato, non verrà segnalato di nuovo per un periodo di tempo configurabile (`OPPORTUNITY_COOLDOWN`).

## Sistema di Notifiche
Il bot utilizza Telegram per fornire aggiornamenti in tempo reale:
- **Notifiche di Opportunità:** Invio immediato di un messaggio quando viene trovata un'opportunità che supera tutti i filtri. Il sistema è stato ottimizzato per inviare una notifica per ogni singola opportunità, dato che il bot è ora sufficientemente selettivo da non causare "spam".
- **Riepilogo Orario:** Ogni ora, il bot invia un messaggio di riepilogo contenente:
  - L'uptime totale del bot.
  - Il numero totale di opportunità profittevoli trovate dall'avvio.
  - Il numero totale di opportunità "quasi profittevoli" (con profitto positivo ma inferiore alla soglia).
- **Notifiche di Stato:** Messaggi di avvio e di errore critico per monitorare la salute del bot.

## Statistiche e Logging
Per la massima trasparenza, il bot fornisce un riepilogo statistico dettagliato alla fine di ogni ciclo di analisi. Questo report, stampato direttamente nella console, è fondamentale per comprendere il comportamento del mercato e l'efficacia dei filtri.

- **Durata Analisi:** Il tempo totale impiegato per il ciclo di calcolo, tipicamente nell'ordine di poche centinaia di millisecondi.
- **Triangoli Validi Trovati:** Il numero di percorsi a 3 passi realmente esistenti sul mercato.
- **Scarti per Strategia:** Quanti percorsi sono stati ignorati perché non partivano da un asset prioritario.
- **Scarti per Fallimento Simulazione:** Il totale dei percorsi scartati durante la simulazione, con un **breakdown dettagliato** per ogni motivo specifico del fallimento:
    - `Liquidità insufficiente`
    - `Valore nozionale minimo non raggiunto`
    - `Quantità minima non raggiunta`
    - `Quantità arrotondata a zero (stepSize)`
    - `Dati di mercato o prezzo mancanti`
- **Scarti per Profitto Troppo Basso:** Il totale dei percorsi simulati con successo ma non profittevoli, con un **breakdown** tra perdite nette e profitti positivi ma inferiori alla soglia impostata.
- **Opportunità Profittevoli Trovate:** Il numero di opportunità che hanno superato tutti i filtri nel ciclo corrente.

I log delle opportunità vengono salvati in due file, con codifica **UTF-8** per la massima compatibilità:
- `profitable_opportunities.txt`: Contiene solo le opportunità che hanno superato tutti i filtri.
- `anomalies.txt`: Contiene le rare opportunità con profitti irrealisticamente alti (es. >50%) per analisi successive.

## Installazione e Avvio

1.  **Clonare il Repository**
    ```bash
    git clone https://github.com/tech-and-finance/ArbitraggioTriangolare.git
    cd ArbitraggioTriangolare
    ```

2.  **Creare un Ambiente Virtuale**
    ```bash
    python -m venv venv
    ```
    Su Windows:
    ```powershell
    .\venv\Scripts\Activate.ps1
    ```
    Su macOS/Linux:
    ```bash
    source venv/bin/activate
    ```

3.  **Installare le Dipendenze**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configurare le Credenziali (Opzionale)**
    Il token del bot Telegram e il chat ID sono già inseriti nel codice. Per una maggiore sicurezza, è consigliabile impostarli come variabili d'ambiente (`TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID`).

5.  **Avviare il Bot**
    ```bash
    python arbitraggio.py
    ```

---
*Disclaimer: Questo strumento è fornito a scopo educativo e sperimentale. L'arbitraggio di criptovalute comporta rischi significativi. L'autore non si assume alcuna responsabilità per eventuali perdite finanziarie. Usare a proprio rischio.* 