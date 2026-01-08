# relop

[![CI](https://github.com/SarthakMakhija/relop/actions/workflows/build.yml/badge.svg)](https://github.com/SarthakMakhija/relop/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/SarthakMakhija/relop/graph/badge.svg?token=U1AAV7UC4J)](https://codecov.io/gh/SarthakMakhija/relop)

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
- [ ] Implement core relational operators:
  - [ ] Scan
  - [ ] Filter
  - [ ] Projection
  - [ ] Join  (incrementally) 
  - [ ] Limit (incrementally)
- [X] Build a minimal in-memory store to mimic relational database storage
  - [X] Tables with schemas
  - [X] Rows stored in memory
  - [X] Simple row identifiers
  - [X] Insert rows via catalog-managed API
  - [X] Row lookup via internal row identifiers
  - [X] Sequential table scan abstraction
  - [X] Primary key index for enforcing uniqueness and lookup
- [ ] Keep the system small, explicit, and easy to reason about.

## Non-goals

- No CREATE / INSERT via SQL
- No persistence or disk I/O
- No physical plans or cost-based optimization
- No full SQL compatibility
