# Titolo del Progetto

Un'applicazione web con un **backend** sviluppato in **Python (PySpark)** e un **frontend** sviluppato con **React**.

## Indice

- [Panoramica](#panoramica)
- [Tecnologie Utilizzate](#tecnologie-utilizzate)
- [Istruzioni per la Configurazione](#istruzioni-per-la-configurazione)
  - [Backend](#backend)
  - [Frontend](#frontend)
- [Istruzioni per l'Esecuzione](#istruzioni-per-lesecuzione)
  - [Esecuzione del Backend](#esecuzione-del-backend)
  - [Esecuzione del Frontend](#esecuzione-del-frontend)

## Panoramica

Questo progetto è stato sviluppato come parte del corso di **Modelli e Tecniche per Big Data** nel corso magistrale di **Artificial Intelligence e Machine Learning**. Utilizza PySpark per l'elaborazione dei dati nel backend e React per un frontend dinamico e intuitivo. Il backend gestisce le operazioni sui dati e le API, mentre il frontend offre un'interfaccia utente fluida per l'interazione.

## Tecnologie Utilizzate

- **Backend**: Python con PySpark
- **Frontend**: React (TypeScript)

## Istruzioni per la Configurazione

### Backend
1. Assicurati di avere **Python 3.x** installato.
2. Configura PySpark nel tuo ambiente.

### Frontend
1. Spostati nella directory `frontend`:
   ```bash
   cd frontend
   ```
2. Installa i pacchetti npm richiesti:
   ```bash
   npm install
   ```

## Istruzioni per l'Esecuzione

### Esecuzione del Backend
1. Spostati nella directory del backend.
2. Esegui il file `main.py` per avviare il server del backend:
   ```bash
   python main.py
   ```

### Esecuzione del Frontend
1. Spostati nella directory `frontend` se non sei già lì:
   ```bash
   cd frontend
   ```
2. Avvia il server di sviluppo:
   ```bash
   npm run dev
   ```

Il comando `npm install` è necessario per scaricare e configurare tutte le dipendenze specificate nel file `package.json`. Successivamente, il comando `npm run dev` avvia il server di sviluppo di React.
