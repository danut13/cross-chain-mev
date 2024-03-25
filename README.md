# Cross-chain MEV detection

This project is part of the "Analyzing the Role of Bridges in Cross-Chain MEV Extraction" Master's Thesis.

Student: Danut Ilisei

Advisor: Burak Öz

Supervisor : Prof. Dr. Florian Matthes

The project is detecting and analyzing cross-chain MEV extraction between Ethereum and Polygon POS using the Polygon POS bridge.

## Dependencies

### Install make tools

https://www.gnu.org/software/make/#download

### Install dependencies

```bash
make init
```

### Format and lint the code

```bash
make code
```

### Detect cross-chain MEV

```bash
make exec
```

### View fetched data

```bash
make view
```

### Delete data

```bash
make delete start=<start_block_number> end=<end_block_number>
```