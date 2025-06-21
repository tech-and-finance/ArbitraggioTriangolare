# Bot di Arbitraggio Triangolare per Binance

Questo progetto è un bot avanzato per il rilevamento di opportunità di arbitraggio triangolare sull'exchange di criptovalute Binance. Il bot si connette in tempo reale ai flussi di dati di mercato, calcola migliaia di potenziali percorsi di arbitraggio al secondo e implementa una serie di filtri strategici per identificare solo le opportunità realistiche e potenzialmente profittevoli.

## Architettura e Design

Il bot è costruito su un'architettura ibrida ad alte prestazioni che sfrutta il meglio di due mondi della programmazione concorrente in Python:

1.  **`asyncio` per l'I/O di Rete:** Il thread principale del programma è gestito da `asyncio`. Questo gli permette di gestire in modo estremamente efficiente centinaia di operazioni di Input/Output simultaneamente, come:
    *   Mantenere aperte e ricevere dati da multiple connessioni WebSocket con Binance.
    *   Scrivere in modo non bloccante su diversi file di log.
    *   Inviare notifiche all'API di Telegram senza "congelare" il resto del programma.

2.  **`ProcessPoolExecutor` per i Calcoli Intensivi:** Il calcolo delle permutazioni di arbitraggio è un'operazione che richiede un uso intensivo della CPU. Se fosse eseguita nel thread principale di `asyncio`, bloccherebbe l'intero programma, causando la disconnessione dai WebSocket. Per evitare ciò, questi calcoli vengono delegati a un pool di processi separati. Questo permette al bot di sfruttare tutti i core della CPU per i calcoli pesanti, mentre il thread principale rimane libero e reattivo per gestire la rete.

Questa architettura garantisce stabilità e performance, prevenendo i timeout delle connessioni anche durante i cicli di analisi più lunghi.

## Strategia e Filtri Implementati

L'efficacia di un bot di arbitraggio non risiede solo nella velocità, ma soprattutto nella sua capacità di distinguere il "rumore" dalle reali opportunità. Per questo, sono stati implementati diversi livelli di filtro, sia tecnici che strategici.

### 1. Filtro di Liquidità (Il più importante)

**Problema:** I calcoli teorici basati solo sui prezzi di mercato spesso mostrano profitti enormi (20-50% o più). Questi profitti sono quasi sempre illusori, causati da una **scarsa liquidità** sul book di ordini. Un trade reale, anche di piccolo importo, altererebbe il prezzo a nostro svantaggio, annullando il profitto.

**Soluzione:** Il bot non si limita a moltiplicare i tassi di cambio. Esegue invece una **simulazione di trade realistica** per ogni potenziale triangolo, utilizzando un capitale di partenza fisso (impostato in `SIMULATION_CAPITAL`). Per ogni passo del triangolo, il bot controlla:
> "La quantità di criptovaluta che devo acquistare/vendere è inferiore alla quantità disponibile sul book a quel dato prezzo?"

Solo se tutti e tre i passaggi dell'arbitraggio superano questo controllo di liquidità, l'opportunità viene considerata valida. Questo scarta oltre il 99% dei falsi positivi.

### 2. Filtro per Percorsi Preferiti

**Problema:** Un'opportunità di profitto su un triangolo (es. A-B-C) è indipendente dal punto di partenza. Questo genera notifiche ridondanti (A→B→C, B→C→A, etc.) e poco pratiche se non si possiede la valuta di partenza.

**Soluzione:** È stata implementata una logica di filtro strategico:
*   **Se un triangolo contiene una stablecoin** (USDT, USDC, etc.), il bot processerà solo i percorsi che **partono e finiscono** con quella stablecoin. Questo permette di accumulare profitti direttamente in valuta stabile.
*   **Se un triangolo non contiene stablecoin** (es. BTC→ETH→BNB), l'opportunità viene comunque processata, in quanto permette di aumentare la quantità di un asset principale.

### 3. De-duplicazione dei Triangoli

**Problema:** Anche con il filtro dei percorsi preferiti, un triangolo tra tre stablecoin (es. USDT-USDC-FDUSD) genererebbe comunque tre notifiche quasi identiche.

**Soluzione:** Il bot tiene traccia dei triangoli già processati all'interno di un singolo ciclo di controllo. Una volta che la prima permutazione profittevole di un triangolo è stata registrata e notificata, tutte le altre permutazioni dello stesso triangolo vengono ignorate. Questo assicura **una sola notifica per ogni reale opportunità di profitto**.

### 4. Cooldown per le Notifiche

**Problema:** In momenti di alta volatilità, il bot potrebbe trovare diverse opportunità valide in pochi secondi, rischiando di essere temporaneamente bloccato dall'API di Telegram per "rate limiting" (troppe richieste).

**Soluzione:** È stato implementato un semplice cooldown (`TELEGRAM_COOLDOWN`) che impedisce l'invio di più di una notifica ogni `X` secondi.

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

4.  **Configurare le Credenziali**
    Apri il file `arbitraggio.py` e inserisci il token del tuo bot Telegram e il tuo chat ID nel dizionario `TELEGRAM_CONFIG`.

5.  **Avviare il Bot**
    ```bash
    python arbitraggio.py
    ```

## Struttura dei File di Log

Il bot genera due tipi di file di log:

*   `arbitrage_YYYYMMDD.txt`: È il **log principale e completo**. Contiene tutte le opportunità trovate che hanno un profitto lordo positivo, incluse quelle "quasi profittevoli" e quelle scartate dal filtro dei percorsi preferiti. Utile per analisi e debug.
*   `profitable_opportunities.txt`: È il **log pulito e azionabile**. Contiene solo le opportunità che hanno superato tutti i filtri (liquidità, percorso preferito) e sono state de-duplicate. Rappresenta l'output finale e più affidabile del bot.

---
*Disclaimer: Questo strumento è fornito a scopo educativo e sperimentale. L'arbitraggio di criptovalute comporta rischi significativi. L'autore non si assume alcuna responsabilità per eventuali perdite finanziarie. Usare a proprio rischio.* 