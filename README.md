# relop

**relop** is a minimal, in-memory implementation of **relational operators** built to explore **query processing** - from lexical analysis and parsing to logical planning and execution.

The project intentionally focuses on a **small subset of SQL SELECT** and operates over **pre-loaded in-memory relations**, avoiding database concerns such as storage, persistence, transactions, or optimization.

**relop** is a learning-focused project inspired by relational algebra and database internals, not a production-ready query engine.

## Goals

- Understand query processing:
  - Grammar  
  - Lexer
  - Parser
  - AST
  - Logical plan
  - Operator-based execution
- Implement core relational operators:
  - Scan
  - Filter
  - Projection
  - Join  (incrementally) 
  - Limit (incrementally)
- Keep the system small, explicit, and easy to reason about.

## Non-goals

- No CREATE / INSERT via SQL
- No persistence or disk I/O
- No physical plans or cost-based optimization
- No full SQL compatibility

