# relop

[![CI](https://github.com/SarthakMakhija/relop/actions/workflows/build.yml/badge.svg)](https://github.com/SarthakMakhija/relop/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/SarthakMakhija/relop/graph/badge.svg?token=U1AAV7UC4J)](https://codecov.io/gh/SarthakMakhija/relop)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square) 

**relop** is a minimal, in-memory implementation of **relational operators** built to explore **query processing** - from lexical analysis and parsing to logical planning and execution.

The project intentionally focuses on a **small subset of SQL SELECT** and operates over **pre-loaded in-memory relations**, avoiding database concerns such as storage, persistence, transactions, or optimization.

**relop** is a learning-focused project inspired by relational algebra and database internals, not a production-ready query engine.

## Goals

- [ ] Understand query processing:
  - [ ] Grammar  
  - [ ] Lexer
  - [ ] Parser
  - [ ] AST
  - [ ] Logical plan
  - [ ] Operator-based execution 
- [ ] Implement core relational operators and metadata queries
  - [X] Scan
  - [ ] Filter
  - [X] Projection
  - [ ] Join  (incrementally) 
  - [X] Limit
  - [ ] Order by
  - [X] Show tables
  - [X] Describe table
- [X] Build a minimal in-memory store to mimic relational database storage
  - [X] Tables with schemas
  - [X] Rows stored in memory
  - [X] Simple row identifiers
  - [X] Insert rows via catalog-managed API
  - [X] Row lookup via internal row identifiers
  - [X] Sequential table scan abstraction
  - [X] Primary key index for enforcing uniqueness and lookup
  - [X] Thin client to demonstrate end-to-end pipeline
- [ ] Keep the system small, explicit, and easy to reason about.

## Non-goals

- No CREATE / INSERT via SQL
- No persistence or disk I/O
- No physical plans or cost-based optimization
- No full SQL compatibility
